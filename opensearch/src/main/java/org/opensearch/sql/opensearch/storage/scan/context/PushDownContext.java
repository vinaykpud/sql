/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.PushDownUnSupportedException;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Push down context is used to store all the push down operations that are applied to the query */
@Getter
public class PushDownContext extends AbstractCollection<PushDownOperation> {
  private static final Logger LOGGER = LogManager.getLogger(PushDownContext.class);

  private final OpenSearchIndex osIndex;
  private ArrayDeque<PushDownOperation> queue = new ArrayDeque<>();
  private ArrayDeque<PushDownOperation> operationsForRequestBuilder;

  private boolean isAggregatePushed = false;
  private AggPushDownAction aggPushDownAction;
  private ArrayDeque<PushDownOperation> operationsForAgg;

  // Records the start pos of the query, which is updated by new added limit operations.
  private int startFrom = 0;

  private boolean isLimitPushed = false;
  private boolean isProjectPushed = false;
  private boolean isMeasureOrderPushed = false;
  private boolean isSortPushed = false;
  private boolean isSortExprPushed = false;
  private boolean isTopKPushed = false;
  private boolean isRareTopPushed = false;
  private boolean isScriptPushed = false;

  public PushDownContext(OpenSearchIndex osIndex) {
    this.osIndex = osIndex;
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
      if (action.type() != PushDownType.SORT && action.type() != PushDownType.SORT_EXPR) {
        newContext.add(action);
      }
    }
    return newContext;
  }

  @NotNull
  @Override
  public Iterator<PushDownOperation> iterator() {
    return queue.iterator();
  }

  @Override
  public int size() {
    return queue.size();
  }

  void addOperationForRequestBuilder(PushDownOperation operation) {
    if (operationsForRequestBuilder == null) {
      this.operationsForRequestBuilder = new ArrayDeque<>();
    }
    operationsForRequestBuilder.add(operation);
    queue.add(operation);
  }

  void addOperationForAgg(PushDownOperation operation) {
    if (operationsForAgg == null) {
      this.operationsForAgg = new ArrayDeque<>();
    }
    operationsForAgg.add(operation);
    queue.add(operation);
  }

  @Override
  public boolean add(PushDownOperation operation) {
    operation.action().pushOperation(this, operation);
    if (operation.type() == PushDownType.AGGREGATION) {
      isAggregatePushed = true;
      this.aggPushDownAction = (AggPushDownAction) operation.action();
    }
    if (operation.type() == PushDownType.LIMIT) {
      startFrom += ((LimitDigest) operation.digest()).offset();
      if (startFrom >= osIndex.getMaxResultWindow()) {
        throw new PushDownUnSupportedException(
            String.format(
                "[INTERNAL] Requested offset %d should be less than the max result window %d",
                startFrom, osIndex.getMaxResultWindow()));
      }
      isLimitPushed = true;
      if (isSortPushed || isMeasureOrderPushed || isSortExprPushed) {
        isTopKPushed = true;
      }
    }
    if (operation.type() == PushDownType.PROJECT) {
      isProjectPushed = true;
    }
    if (operation.type() == PushDownType.SORT) {
      isSortPushed = true;
    }
    if (operation.type() == PushDownType.SORT_EXPR) {
      isSortExprPushed = true;
    }
    if (operation.type() == PushDownType.SORT_AGG_METRICS) {
      isMeasureOrderPushed = true;
    }
    if (operation.type() == PushDownType.RARE_TOP) {
      isRareTopPushed = true;
    }
    if (operation.type() == PushDownType.SCRIPT) {
      isScriptPushed = true;
    }
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

  /**
   * Get the digest of the first operation of a specific type.
   *
   * @param type The PushDownType to get the digest for
   * @return The digest object, or null if no operation of the specified type exists
   */
  public Object getDigestByType(PushDownType type) {
    return this.stream()
        .filter(operation -> operation.type() == type)
        .map(PushDownOperation::digest)
        .findFirst()
        .orElse(null);
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

      LOGGER.info("Starting reconstruction with {} operations", pushDownOperations.size());
      for (int i = 0; i < pushDownOperations.size(); i++) {
          PushDownOperation op = pushDownOperations.get(i);
          LOGGER.info("Operation {}: type={}, digest={}", i, op.type(), op.digest());
          RelNode storedRelNode = op.relNode();
          if (storedRelNode != null) {
              LOGGER.info("  Stored RelNode: {}", storedRelNode);
              RelNode before = current;
              current = replaceInput(storedRelNode, current);
              LOGGER.info("  {} being added as input to {}", before, current);
          } else {
              LOGGER.info("  No RelNode stored for this operation");
          }
      }
      LOGGER.info("Final reconstructed tree\n: {}", current);
      return current;
  }

  /**
   * Replace the input of a RelNode with a new input.
   * Creates a new RelNode instance with the updated input while preserving the exact structure.
   * Uses direct copy() methods to avoid Calcite optimizations that change the condition structure.
   */
  private RelNode replaceInput(RelNode relNode, RelNode newInput) {

    if (relNode instanceof LogicalFilter filter) {
      // Use direct copy() to preserve exact filter condition (avoid SEARCH optimization)
      return filter.copy(filter.getTraitSet(), newInput, filter.getCondition());
    }

    if (relNode instanceof LogicalAggregate agg) {
      // Use direct copy() to preserve exact aggregate structure
      return agg.copy(
          agg.getTraitSet(),
          newInput,
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());
    }

    if (relNode instanceof LogicalProject proj) {
      // Use direct copy() to preserve exact project expressions
      return proj.copy(
          proj.getTraitSet(),
          newInput,
          proj.getProjects(),
          proj.getRowType());
    }

    if (relNode instanceof LogicalSort sort) {
      // Use direct copy() to preserve exact sort collation
      return sort.copy(
          sort.getTraitSet(),
          newInput,
          sort.getCollation(),
          sort.offset,
          sort.fetch);
    }

    // If we don't know how to handle this RelNode type, throw an exception
    throw new UnsupportedOperationException(
        "Unsupported RelNode type for reconstruction: " + relNode.getClass().getName());
  }
}
