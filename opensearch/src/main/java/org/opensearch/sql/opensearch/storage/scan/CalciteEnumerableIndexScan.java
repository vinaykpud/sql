/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.OpenSearchRules;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/** The physical relational operator representing a scan of an OpenSearchIndex type. */
public class CalciteEnumerableIndexScan extends AbstractCalciteIndexScan
    implements Scannable, EnumerableRel {
  private static final Logger LOG = LogManager.getLogger(CalciteEnumerableIndexScan.class);

  /**
   * Creates an CalciteOpenSearchIndexScan.
   *
   * @param cluster Cluster
   * @param table Table
   * @param osIndex OpenSearch index
   */
  public CalciteEnumerableIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  @Override
  protected AbstractCalciteIndexScan buildScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    return new CalciteEnumerableIndexScan(
        cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  @Override
  public AbstractCalciteIndexScan copy() {
    return new CalciteEnumerableIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  @Override
  public void register(RelOptPlanner planner) {
    for (RelOptRule rule : OpenSearchRules.OPEN_SEARCH_OPT_RULES) {
      planner.addRule(rule);
    }

    // remove this rule otherwise opensearch can't correctly interpret approx_count_distinct()
    // it is converted to cardinality aggregation in OpenSearch
    planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);

    // Remove FILTER_REDUCE_EXPRESSIONS rule to prevent conversion of range comparisons to SEARCH
    // This is needed for Substrait compatibility which doesn't support SEARCH operations
    planner.removeRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    /* In Calcite enumerable operators, row of single column will be optimized to a scalar value.
     * See {@link PhysTypeImpl}.
     * Since we need to combine this operator with their original ones,
     * let's follow this convention to apply the optimization here and ensure `scan` method
     * returns the correct data format for single column rows.
     * See {@link OpenSearchIndexEnumerator}
     * Besides, we replace all dots in fields to avoid the Calcite codegen bug.
     * https://github.com/opensearch-project/sql/issues/4619
     */
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), getRowType()),
            pref.preferArray());

    Expression scanOperator = implementor.stash(this, CalciteEnumerableIndexScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  /**
   * This Enumerator may be iterated for multiple times, so we need to create opensearch request for
   * each time to avoid reusing source builder. That's because the source builder has stats like PIT
   * or SearchAfter recorded during previous search.
   */
  @Override
  public Enumerable<@Nullable Object> scan() {
    // Reconstruct pushed-down RelNode tree
    RelNode pushedDownTree = null;
    RelNode fullRelNodeTree = null;
    try {
      if (pushDownContext != null && !pushDownContext.isEmpty()) {
        fullRelNodeTree = CalciteToolsHelper.OpenSearchRelRunners.getCurrentRelNode();
        CalciteToolsHelper.OpenSearchRelRunners.clearCurrentRelNode();
        LOG.info("Full RelNode tree:\n{}", RelOptUtil.toString(fullRelNodeTree));
        LOG.info("=== PushDownContext contains {} operations ===", pushDownContext.size());
        int index = 0;
        for (var operation : pushDownContext) {
          LOG.info("  Operation {}: type={}, relNode={}",
              index++,
              operation.type(),
              operation.relNode() != null ? operation.relNode().toString() : "NULL");
        }

        // Create a base CalciteLogicalIndexScan for reconstruction
        CalciteLogicalIndexScan logicalIndexScan = new CalciteLogicalIndexScan(getCluster(), getTable(), osIndex);
        pushedDownTree = pushDownContext.reconstructPushedDownRelNodeTree(logicalIndexScan);
        LOG.info("Reconstructed pushed-down RelNode tree:\n{}", pushedDownTree.explain());
      }
    } catch (Exception e) {
      LOG.error("Failed to reconstruct pushed-down RelNode tree", e);
      throw new RuntimeException("Failed to reconstruct pushed-down RelNode tree", e);
    }

    // Pass pushedDownTree to OpenSearchRequest via RequestBuilder
    final RelNode finalPushedDownTree = pushedDownTree;
    // If needed for testing use finalFullRelNodeTree
    final RelNode finalFullRelNodeTree = fullRelNodeTree;

    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        OpenSearchRequestBuilder requestBuilder = pushDownContext.createRequestBuilder();
        // Set the RelNode tree on the request builder
        if (finalPushedDownTree != null) {
          requestBuilder.setPushedDownRelNodeTree(finalPushedDownTree);
        }
        return new OpenSearchIndexEnumerator(
            osIndex.getClient(),
            getRowType().getFieldNames(),
            requestBuilder.getMaxResponseSize(),
            requestBuilder.getMaxResultWindow(),
            osIndex.getQueryBucketSize(),
            osIndex.buildRequest(requestBuilder),
            osIndex.createOpenSearchResourceMonitor());
      }
    };
  }
}
