/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import com.google.common.collect.Iterators;

import java.sql.Connection;
import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Push down context is used to store all the push down operations that are applied to the query */
@Getter
public class PushDownContext extends AbstractCollection<PushDownOperation> {
  private static final Logger LOGGER = LogManager.getLogger(PushDownContext.class);
  
  private final OpenSearchIndex osIndex;
  private final OpenSearchRequestBuilder requestBuilder;
  private ArrayDeque<PushDownOperation> operationsForRequestBuilder;

  private boolean isAggregatePushed = false;
  private AggPushDownAction aggPushDownAction;
  private ArrayDeque<PushDownOperation> operationsForAgg;

  private boolean isLimitPushed = false;
  private boolean isProjectPushed = false;
  private boolean isMeasureOrderPushed = false;
  private boolean isSortPushed = false;
  private boolean isTopKPushed = false;
  private boolean isRareTopPushed = false;

  public PushDownContext(OpenSearchIndex osIndex) {
    this.osIndex = osIndex;
    this.requestBuilder = osIndex.createRequestBuilder();
  }

  @Override
  public PushDownContext clone() {
    PushDownContext newContext = new PushDownContext(osIndex);
    newContext.addAll(this);
    return newContext;
  }

  /**
   * Create a new {@link PushDownContext} without the collation action.
   *
   * @return A new push-down context without the collation action.
   */
  public PushDownContext cloneWithoutSort() {
    PushDownContext newContext = new PushDownContext(osIndex);
    for (PushDownOperation action : this) {
      if (action.type() != PushDownType.SORT) {
        newContext.add(action);
      }
    }
    return newContext;
  }

  @NotNull
  @Override
  public Iterator<PushDownOperation> iterator() {
    if (operationsForRequestBuilder == null) {
      return Collections.emptyIterator();
    } else if (operationsForAgg == null) {
      return operationsForRequestBuilder.iterator();
    } else {
      return Iterators.concat(operationsForRequestBuilder.iterator(), operationsForAgg.iterator());
    }
  }

  @Override
  public int size() {
    return (operationsForRequestBuilder == null ? 0 : operationsForRequestBuilder.size())
        + (operationsForAgg == null ? 0 : operationsForAgg.size());
  }

  ArrayDeque<PushDownOperation> getOperationsForRequestBuilder() {
    if (operationsForRequestBuilder == null) {
      this.operationsForRequestBuilder = new ArrayDeque<>();
    }
    return operationsForRequestBuilder;
  }

  ArrayDeque<PushDownOperation> getOperationsForAgg() {
    if (operationsForAgg == null) {
      this.operationsForAgg = new ArrayDeque<>();
    }
    return operationsForAgg;
  }

  @Override
  public boolean add(PushDownOperation operation) {
    if (operation.type() == PushDownType.AGGREGATION) {
      isAggregatePushed = true;
      this.aggPushDownAction = (AggPushDownAction) operation.action();
    }
    if (operation.type() == PushDownType.LIMIT) {
      isLimitPushed = true;
      if (isSortPushed || isMeasureOrderPushed) {
        isTopKPushed = true;
      }
    }
    if (operation.type() == PushDownType.PROJECT) {
      isProjectPushed = true;
    }
    if (operation.type() == PushDownType.SORT) {
      isSortPushed = true;
    }
    if (operation.type() == PushDownType.SORT_AGG_METRICS) {
      isMeasureOrderPushed = true;
    }
    if (operation.type() == PushDownType.RARE_TOP) {
      isRareTopPushed = true;
    }
    operation.action().transform(this, operation);
    return true;
  }

  public void add(PushDownType type, Object digest, AbstractAction<?> action) {
    add(new PushDownOperation(type, digest, action));
  }
  
  public void add(PushDownType type, Object digest, AbstractAction<?> action, RelNode relNode) {
    add(new PushDownOperation(type, digest, action, relNode));
  }

  public boolean containsDigest(Object digest) {
    return this.stream().anyMatch(action -> action.digest().equals(digest));
  }

  public OpenSearchRequestBuilder createRequestBuilder() {
    OpenSearchRequestBuilder newRequestBuilder = osIndex.createRequestBuilder();
    if (operationsForRequestBuilder != null) {
      operationsForRequestBuilder.forEach(
          operation -> ((OSRequestBuilderAction) operation.action()).apply(newRequestBuilder));
    }
    return newRequestBuilder;
  }
  
  /**
   * Reconstruct the complete pushed-down RelNode tree from stored operations.
   * Builds: Scan → Filter → Project → Aggregate → Limit
   * 
   * @param logicalIndexScan The base CalciteLogicalIndexScan to start from
   * @return The complete RelNode tree with all push-downs applied
   */
  public RelNode reconstructPushedDownRelNodeTree(RelNode logicalIndexScan) {
      RelNode current = logicalIndexScan;
      List<PushDownOperation> pushDownOperations = new ArrayList<>(this);

      for (PushDownOperation pushDownOperation : pushDownOperations) {
          RelNode storedRelNode = pushDownOperation.relNode();
          if (storedRelNode != null) {
              LOGGER.info("RelNode: {}", storedRelNode);
              RelNode before = current;
              current = replaceInput(storedRelNode, current);
              LOGGER.info("{} being added as input to {}", before, current);
          }
      }
      return current;
  }
  
  /**
   * Replace the input of a RelNode with a new input.
   * Creates a new RelNode instance with the updated input and properly derived row type.
   * Uses RelBuilder to ensure proper row type derivation.
   */
  private RelNode replaceInput(RelNode relNode, RelNode newInput) {
    
    // Create a RelBuilder for proper row type derivation
    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .typeSystem(OpenSearchTypeSystem.INSTANCE)
            .build();
    Connection connection = CalciteToolsHelper.connect(config, OpenSearchTypeFactory.TYPE_FACTORY);
    RelBuilder builder =
        CalciteToolsHelper.create(
            config, 
            OpenSearchTypeFactory.TYPE_FACTORY,
            connection);
    
    builder.push(newInput);
    
    if (relNode instanceof LogicalFilter filter) {
        // Use RelBuilder.filter() to properly derive row type
      builder.filter(filter.getCondition());
      return builder.build();
    }
    
    if (relNode instanceof LogicalAggregate agg) {

        // Convert AggregateCall list to RelBuilder.AggCall list
      List<RelBuilder.AggCall> aggCalls =
          new java.util.ArrayList<>();
      for (AggregateCall aggCall : agg.getAggCallList()) {
        aggCalls.add(
            builder.aggregateCall(
                aggCall.getAggregation(),
                aggCall.getArgList().stream()
                    .map(builder::field)
                    .collect(Collectors.toList()))
                .distinct(aggCall.isDistinct())
                .as(aggCall.getName()));
      }
      
      // Use RelBuilder.aggregate() to properly derive row type
      builder.aggregate(
          builder.groupKey(agg.getGroupSet(), agg.getGroupSets()),
          aggCalls);
      return builder.build();
    }
    
    if (relNode instanceof LogicalProject proj) {
        // Use RelBuilder.project() to properly derive row type
      builder.project(proj.getProjects(), proj.getRowType().getFieldNames());
      return builder.build();
    }
    
    if (relNode instanceof LogicalSort sort) {
        // Use RelBuilder.sort() to properly derive row type
      builder.sortLimit(
          sort.offset != null ? Objects.requireNonNull(((RexLiteral) sort.offset).getValueAs(Integer.class)) : -1,
          sort.fetch != null ? Objects.requireNonNull(((RexLiteral) sort.fetch).getValueAs(Integer.class)) : -1,
          builder.fields(sort.getCollation()));
      return builder.build();
    }

    // If we don't know how to handle this RelNode type, throw an exception
    throw new UnsupportedOperationException(
        "Unsupported RelNode type for reconstruction: " + relNode.getClass().getName());
  }
}
