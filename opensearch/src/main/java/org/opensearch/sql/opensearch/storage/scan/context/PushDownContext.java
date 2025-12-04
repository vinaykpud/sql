/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import com.google.common.collect.Iterators;
import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Push down context is used to store all the push down operations that are applied to the query */
@Getter
public class PushDownContext extends AbstractCollection<PushDownOperation> {
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
  
  public void add(PushDownType type, Object digest, AbstractAction<?> action, org.apache.calcite.rel.RelNode relNode) {
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
   * @param baseScan The base CalciteLogicalIndexScan to start from
   * @return The complete RelNode tree with all push-downs applied
   */
  public org.apache.calcite.rel.RelNode reconstructPushedDownRelNodeTree(
      org.apache.calcite.rel.RelNode baseScan) {
    org.apache.calcite.rel.RelNode current = baseScan;
    
    // Convert to list for lookahead
    java.util.List<PushDownOperation> operations = new java.util.ArrayList<>();
    this.forEach(operations::add);
    
    int step = 0;
    for (int i = 0; i < operations.size(); i++) {
      PushDownOperation operation = operations.get(i);
      org.apache.calcite.rel.RelNode storedRelNode = operation.relNode();
      if (storedRelNode != null) {
        org.apache.calcite.rel.RelNode before = current;
        current = replaceInput(storedRelNode, current);
        System.out.println(String.format("  Step %d: Applied %s", step, operation.type()));
        System.out.println(String.format("    Before: %s", before));
        System.out.println(String.format("    After:  %s", current));
        step++;
      }
    }
    
    return current;
  }
  
  /**
   * Replace the input of a RelNode with a new input.
   * Creates a new RelNode instance with the updated input and properly derived row type.
   * Uses RelBuilder to ensure proper row type derivation.
   */
  private org.apache.calcite.rel.RelNode replaceInput(
      org.apache.calcite.rel.RelNode relNode, 
      org.apache.calcite.rel.RelNode newInput) {
    
    // Create a RelBuilder for proper row type derivation
    org.apache.calcite.tools.FrameworkConfig config = 
        org.apache.calcite.tools.Frameworks.newConfigBuilder()
            .typeSystem(org.opensearch.sql.executor.OpenSearchTypeSystem.INSTANCE)
            .build();
    java.sql.Connection connection = 
        org.opensearch.sql.calcite.utils.CalciteToolsHelper.connect(
            config, 
            org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY);
    org.apache.calcite.tools.RelBuilder builder = 
        org.opensearch.sql.calcite.utils.CalciteToolsHelper.create(
            config, 
            org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY, 
            connection);
    
    builder.push(newInput);
    
    if (relNode instanceof org.apache.calcite.rel.logical.LogicalFilter) {
      org.apache.calcite.rel.logical.LogicalFilter filter = 
          (org.apache.calcite.rel.logical.LogicalFilter) relNode;
      // Use RelBuilder.filter() to properly derive row type
      builder.filter(filter.getCondition());
      return builder.build();
    }
    
    if (relNode instanceof org.apache.calcite.rel.logical.LogicalAggregate) {
      org.apache.calcite.rel.logical.LogicalAggregate agg = 
          (org.apache.calcite.rel.logical.LogicalAggregate) relNode;
      
      // Convert AggregateCall list to RelBuilder.AggCall list
      java.util.List<org.apache.calcite.tools.RelBuilder.AggCall> aggCalls = 
          new java.util.ArrayList<>();
      for (org.apache.calcite.rel.core.AggregateCall aggCall : agg.getAggCallList()) {
        aggCalls.add(
            builder.aggregateCall(
                aggCall.getAggregation(),
                aggCall.getArgList().stream()
                    .map(builder::field)
                    .collect(java.util.stream.Collectors.toList()))
                .distinct(aggCall.isDistinct())
                .as(aggCall.getName()));
      }
      
      // Use RelBuilder.aggregate() to properly derive row type
      builder.aggregate(
          builder.groupKey(agg.getGroupSet(), agg.getGroupSets()),
          aggCalls);
      return builder.build();
    }
    
    if (relNode instanceof org.apache.calcite.rel.logical.LogicalProject) {
      org.apache.calcite.rel.logical.LogicalProject proj = 
          (org.apache.calcite.rel.logical.LogicalProject) relNode;
      // Use RelBuilder.project() to properly derive row type
      builder.project(proj.getProjects(), proj.getRowType().getFieldNames());
      return builder.build();
    }
    
    if (relNode instanceof org.apache.calcite.rel.logical.LogicalSort) {
      org.apache.calcite.rel.logical.LogicalSort sort = 
          (org.apache.calcite.rel.logical.LogicalSort) relNode;
      // Use RelBuilder.sort() to properly derive row type
      builder.sortLimit(
          sort.offset != null ? ((org.apache.calcite.rex.RexLiteral) sort.offset).getValueAs(Integer.class) : -1,
          sort.fetch != null ? ((org.apache.calcite.rex.RexLiteral) sort.fetch).getValueAs(Integer.class) : -1,
          builder.fields(sort.getCollation()));
      return builder.build();
    }
    
    // Fallback code commented out - we only handle Logical* types with RelBuilder
    // If we encounter other types, throw an exception to make it explicit
    /*
    // Fallback to copy() for other RelNode types
    if (relNode instanceof org.apache.calcite.rel.core.Filter) {
      org.apache.calcite.rel.core.Filter filter = (org.apache.calcite.rel.core.Filter) relNode;
      return filter.copy(filter.getTraitSet(), newInput, filter.getCondition());
    }
    
    if (relNode instanceof org.apache.calcite.rel.core.Aggregate) {
      org.apache.calcite.rel.core.Aggregate agg = (org.apache.calcite.rel.core.Aggregate) relNode;
      return agg.copy(
          agg.getTraitSet(),
          newInput,
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());
    }
    
    if (relNode instanceof org.apache.calcite.rel.core.Project) {
      org.apache.calcite.rel.core.Project proj = (org.apache.calcite.rel.core.Project) relNode;
      return proj.copy(proj.getTraitSet(), newInput, proj.getProjects(), proj.getRowType());
    }
    
    if (relNode instanceof org.apache.calcite.rel.core.Sort) {
      org.apache.calcite.rel.core.Sort sort = (org.apache.calcite.rel.core.Sort) relNode;
      return sort.copy(
          sort.getTraitSet(),
          newInput,
          sort.getCollation(),
          sort.offset,
          sort.fetch);
    }
    */
    
    // If we don't know how to handle this RelNode type, throw an exception
    throw new UnsupportedOperationException(
        "Unsupported RelNode type for reconstruction: " + relNode.getClass().getName());
  }
}
