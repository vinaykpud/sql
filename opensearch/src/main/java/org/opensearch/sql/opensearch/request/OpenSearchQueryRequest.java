/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.UserTypeMapper;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.EmptyVisitationContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST;
import static org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

/**
 * OpenSearch search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation. 2)
 * Indicate the search already done.
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchQueryRequest implements OpenSearchRequest {

  private static final SimpleExtension.ExtensionCollection EXTENSIONS = DefaultExtensionCatalog.DEFAULT_COLLECTION;

  public static final String INJECTED_COUNT_AGGREGATE_NAME = "agg_for_doc_count";

  /** {@link OpenSearchRequest.IndexName}. */
  private final IndexName indexName;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder;

  /** OpenSearchExprValueFactory. */
  @EqualsAndHashCode.Exclude @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /** List of includes expected in the response. */
  @EqualsAndHashCode.Exclude @ToString.Exclude private final List<String> includes;

  @EqualsAndHashCode.Exclude private boolean needClean = true;

  /** Indicate the search already done. */
  private boolean searchDone = false;

  private String pitId;

  private TimeValue cursorKeepAlive;

  private Object[] searchAfter;

  private SearchResponse searchResponse = null;

    private static final Logger LOGGER =
            LogManager.getLogger(OpenSearchQueryRequest.class);

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      String indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
    this(new IndexName(indexName), size, factory, includes);
  }

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      IndexName indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.from(0);
    sourceBuilder.size(size);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
    this.exprValueFactory = factory;
    this.includes = includes;
  }

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes) {
    this.indexName = indexName;
    this.sourceBuilder = sourceBuilder;
    this.exprValueFactory = factory;
    this.includes = includes;
  }

  /** Constructor of OpenSearchQueryRequest with PIT support. */
  public OpenSearchQueryRequest(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes,
      TimeValue cursorKeepAlive,
      String pitId) {
    this.indexName = indexName;
    this.sourceBuilder = sourceBuilder;
    this.exprValueFactory = factory;
    this.includes = includes;
    this.cursorKeepAlive = cursorKeepAlive;
    this.pitId = pitId;
  }

  /** true if the request is a count aggregation request. */
  public boolean isCountAggRequest() {
    return !searchDone
        && sourceBuilder.size() == 0
        && sourceBuilder.trackTotalHitsUpTo() != null // only set in v3
        && sourceBuilder.trackTotalHitsUpTo() == Integer.MAX_VALUE;
  }

  /**
   * Constructs OpenSearchQueryRequest from serialized representation.
   *
   * @param in stream to read data from.
   * @param engine OpenSearchSqlEngine to get node-specific context.
   * @throws IOException thrown if reading from input {@code in} fails.
   */
  public OpenSearchQueryRequest(StreamInput in, OpenSearchStorageEngine engine) throws IOException {
    // Deserialize the SearchSourceBuilder from the string representation
    String sourceBuilderString = in.readString();

    NamedXContentRegistry xContentRegistry =
        new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(xContentRegistry, IGNORE_DEPRECATIONS, sourceBuilderString);
    this.sourceBuilder = SearchSourceBuilder.fromXContent(parser);

    cursorKeepAlive = in.readTimeValue();
    pitId = in.readString();
    includes = in.readStringList();
    indexName = new IndexName(in);

    int length = in.readVInt();
    this.searchAfter = new Object[length];
    for (int i = 0; i < length; i++) {
      this.searchAfter[i] = in.readGenericValue();
    }

    OpenSearchIndex index = (OpenSearchIndex) engine.getTable(null, indexName.toString());
    exprValueFactory =
        new OpenSearchExprValueFactory(
            index.getFieldOpenSearchTypes(), index.isFieldTypeTolerance());
  }

  @Override
  public OpenSearchResponse search(
      Function<SearchRequest, SearchResponse> searchAction,
      Function<SearchScrollRequest, SearchResponse> scrollAction) {
    if (this.pitId == null) {
      // When SearchRequest doesn't contain PitId, fetch single page request
      if (searchDone) {
        return new OpenSearchResponse(
            SearchHits.empty(), exprValueFactory, includes, isCountAggRequest());
      } else {
        // get the value before set searchDone = true
        boolean isCountAggRequest = isCountAggRequest();
        searchDone = true;
        sourceBuilder.queryPlanIR(convertToSubstraitAndSerialize(exprValueFactory));
        return new OpenSearchResponse(
            searchAction.apply(
                new SearchRequest().indices(indexName.getIndexNames()).source(sourceBuilder)),
            exprValueFactory,
            includes,
            isCountAggRequest);
      }
    } else {
      // Search with PIT instead of scroll API
      return searchWithPIT(searchAction);
    }
  }

  public OpenSearchResponse searchWithPIT(Function<SearchRequest, SearchResponse> searchAction) {
    OpenSearchResponse openSearchResponse;
    if (searchDone) {
      openSearchResponse =
          new OpenSearchResponse(
              SearchHits.empty(), exprValueFactory, includes, isCountAggRequest());
    } else {
      this.sourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(this.pitId));
      sourceBuilder.queryPlanIR(convertToSubstraitAndSerialize(exprValueFactory));
      this.sourceBuilder.timeout(cursorKeepAlive);
      // check for search after
      if (searchAfter != null) {
        this.sourceBuilder.searchAfter(searchAfter);
      }
      // Add sort tiebreaker for PIT search.
      // We cannot remove it since `_shard_doc` is not added implicitly in PIT now.
      // Ref https://github.com/opensearch-project/OpenSearch/pull/18924#issuecomment-3342365950
      if (this.sourceBuilder.sorts() == null || this.sourceBuilder.sorts().isEmpty()) {
        // If no sort field specified, sort by `_doc` + `_shard_doc`to get better performance
        this.sourceBuilder.sort(DOC_FIELD_NAME, ASC);
//        this.sourceBuilder.sort(SortBuilders.shardDocSort());
      } else {
        // If sort fields specified, sort by `fields` + `_doc` + `_shard_doc`.
        if (this.sourceBuilder.sorts().stream()
            .noneMatch(
                b -> b instanceof FieldSortBuilder f && f.fieldName().equals(DOC_FIELD_NAME))) {
          this.sourceBuilder.sort(DOC_FIELD_NAME, ASC);
        }
//        if (this.sourceBuilder.sorts().stream().noneMatch(ShardDocSortBuilder.class::isInstance)) {
//          this.sourceBuilder.sort(SortBuilders.shardDocSort());
//        }
      }
      SearchRequest searchRequest =
          new SearchRequest().indices(indexName.getIndexNames()).source(this.sourceBuilder);
      this.searchResponse = searchAction.apply(searchRequest);

      openSearchResponse =
          new OpenSearchResponse(
              this.searchResponse, exprValueFactory, includes, isCountAggRequest());

      needClean = openSearchResponse.isEmpty();
      searchDone = openSearchResponse.isEmpty();
      SearchHit[] searchHits = this.searchResponse.getHits().getHits();
      if (searchHits != null && searchHits.length > 0) {
        searchAfter = searchHits[searchHits.length - 1].getSortValues();
        this.sourceBuilder.searchAfter(searchAfter);
      }
    }
    return openSearchResponse;
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      // clean on the last page only, to prevent deleting the PitId in the middle of paging.
      if (this.pitId != null && needClean) {
        cleanAction.accept(this.pitId);
        searchDone = true;
      }
    } finally {
      this.pitId = null;
    }
  }

  @Override
  public void forceClean(Consumer<String> cleanAction) {
    try {
      if (this.pitId != null) {
        cleanAction.accept(this.pitId);
        searchDone = true;
      }
    } finally {
      this.pitId = null;
    }
  }

  @Override
  public boolean hasAnotherBatch() {
    if (this.pitId != null) {
      return !needClean;
    }
    return false;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    if (this.pitId != null) {
      // Convert SearchSourceBuilder to XContent and write it as a string
      out.writeString(sourceBuilder.toString());

      out.writeTimeValue(sourceBuilder.timeout());
      out.writeString(sourceBuilder.pointInTimeBuilder().getId());
      out.writeStringCollection(includes);
      indexName.writeTo(out);

      // Serialize the searchAfter array
      if (searchAfter != null) {
        out.writeVInt(searchAfter.length);
        for (Object obj : searchAfter) {
          out.writeGenericValue(obj);
        }
      }
    } else {
      // OpenSearch Query request without PIT for single page requests
      throw new UnsupportedOperationException(
          "OpenSearchQueryRequest serialization is not implemented.");
    }
  }

    public static byte[] convertToSubstraitAndSerialize(OpenSearchExprValueFactory index) {
        RelNode relNode = CalciteToolsHelper.OpenSearchRelRunners.getCurrentRelNode();

        LOGGER.info("Calcite Logical Plan before Conversion\n {}", RelOptUtil.toString(relNode));

        // Preprocess the Calcite plan
        // Adds a count aggregate if absent for Coordinator merging using doc_count to work
        relNode = ensureCountAggregate(relNode);
        // Support to convert average into sum and count aggs else merging at Coordinator won't work.
        relNode = convertAvgToSumCount(relNode);
        // Support to convert span
        relNode = convertSpan(relNode);
        // Support to convert ILIKE
        relNode = convertILike(relNode);
        // Support to convert Extract
        relNode = convertExtract(relNode);
        // Convert timestamp UDTs to SQL TIMESTAMP types
        relNode = convertTimestamp(relNode);

        LOGGER.info("Calcite Logical Plan after Conversion\n {}", RelOptUtil.toString(relNode));

        long startTimeSubstrait = System.nanoTime();
        // Substrait conversion
        // RelRoot represents the root of a relational query tree with metadata
        RelRoot root = RelRoot.of(relNode, SqlKind.SELECT);

        // Convert using custom visitor to handle EXTRACT and other custom functions
        SubstraitRelVisitor visitor = createVisitor(relNode);
        Rel substraitRel = visitor.apply(root.rel);

        // Build Plan.Root with proper field names from RelRoot
        // otherwise the output column names won't match the query
        List<String> fieldNames = root.fields.stream()
            .map(field -> field.getValue())
            .collect(Collectors.toList());
        Plan.Root substraitRoot = Plan.Root.builder()
            .input(substraitRel)
            .names(fieldNames)
            .build();

        long endTimeSubstraitConvert = System.nanoTime();
        // Plan contains one or more roots (query entry points) and shared extensions
        // addRoots() adds the converted relation tree as a query root
        Plan plan = Plan.builder().addRoots(substraitRoot).build();
        // The Plan now contains two table names like bellow
        //        named_table {
        //            names: "OpenSearch"
        //            names: "hits"
        //        }
        // we want to remove "OpenSearch" as table name for now otherwise execution fails in DF
        TableNameModifier modifier = new TableNameModifier();
        Plan modifiedPlan = modifier.modifyTableNames(plan);

        // Convert to Protocol Buffer format for serialization
        // PlanProtoConverter handles the conversion from Java objects to protobuf
        // This enables serialization, storage, and cross-system communication
        PlanProtoConverter planProtoConverter = new PlanProtoConverter();
        io.substrait.proto.Plan substraitPlanProtoModified = planProtoConverter.toProto(modifiedPlan);
        LOGGER.info("Time taken to convert to Substrait convert (ms) {}", (endTimeSubstraitConvert-startTimeSubstrait)/1000000);
        LOGGER.info("Substrait Logical Plan \n {}", substraitPlanProtoModified.toString());
        return substraitPlanProtoModified.toByteArray();
    }

    private static SubstraitRelVisitor createVisitor(RelNode relNode) {
        List<FunctionMappings.Sig> customSigs = List.of(new FunctionMappings.Sig(
                SqlStdOperatorTable.EXTRACT, "EXTRACT"
        ));

        TypeConverter typeConverter = new TypeConverter(
                new UserTypeMapper() {
                    @Nullable
                    @Override
                    public Type toSubstrait(RelDataType relDataType) {
                        String fullTypeString = relDataType.getFullTypeString();
                        Class<? extends RelDataType> aClass = relDataType.getClass();
                        System.out.println(aClass);
                        System.out.println(relDataType);
                        SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
                        if (fullTypeString.equals("EXPR_TIMESTAMP VARCHAR")) {
                            TypeCreator creator = Type.withNullability(relDataType.isNullable());
                            return creator.precisionTimestamp(3);
                        }
                        return null;
                    }

                    @Nullable
                    @Override
                    public RelDataType toCalcite(Type.UserDefined type) {
                        return null;
                    }
                });

        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
                EXTENSIONS.aggregateFunctions(),
                typeFactory
        );
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
                EXTENSIONS.scalarFunctions(),
                customSigs,
                typeFactory,
                typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
                EXTENSIONS.windowFunctions(),
                typeFactory
        );

        return new SubstraitRelVisitor(
                typeFactory,
                scalarConverter,
                aggConverter,
                windowConverter,
                typeConverter,
                ImmutableFeatureBoard.builder().build());
    }

    private static class TableNameModifier {
        public Plan modifyTableNames(Plan plan) {
            TableNameVisitor visitor = new TableNameVisitor();

            // Transform each root in the plan
            List<Plan.Root> modifiedRoots = new java.util.ArrayList<>();

            for (Plan.Root root : plan.getRoots()) {
                Optional<Rel> modifiedRel = root.getInput().accept(visitor, null);
                if (modifiedRel.isPresent()) {
                    modifiedRoots.add(Plan.Root.builder().from(root).input(modifiedRel.get()).build());
                } else {
                    modifiedRoots.add(root);
                }
            }

            return Plan.builder().from(plan).roots(modifiedRoots).build();
        }

        private static class TableNameVisitor extends RelCopyOnWriteVisitor<RuntimeException> {
            @Override
            public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
                List<String> currentNames = namedScan.getNames();

                // Filter out names that contain "OpenSearch"
                List<String> filteredNames = currentNames.stream()
                    .filter(name -> !name.contains("OpenSearch"))
                    .collect(java.util.stream.Collectors.toList());

                // Only create a new NamedScan if names were actually filtered
                if (filteredNames.size() != currentNames.size() && !filteredNames.isEmpty()) {
                    return Optional.of(
                            NamedScan.builder()
                                    .from(namedScan)
                                    .names(filteredNames)
                                    .build());
                }

                return super.visit(namedScan, context);
            }
        }
    }

  private static RelNode convertAvgToSumCount(RelNode relNode) {
      // Track: original AVG field index → (new SUM index, new COUNT index)
      Map<Integer, Pair<Integer, Integer>> avgFieldMapping = new HashMap<>();

      return relNode.accept(
              new RelShuttleImpl() {

                  @Override
                  public RelNode visit(LogicalAggregate aggregate) {
                      RelNode newInput = aggregate.getInput().accept(this);

                      boolean hasAvg =
                              aggregate.getAggCallList().stream()
                                      .anyMatch(call -> call.getAggregation().getKind() == SqlKind.AVG);

                      if (!hasAvg) {
                          return aggregate.copy(aggregate.getTraitSet(), Collections.singletonList(newInput));
                      }

                      RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build());
                      builder.push(newInput);

                      List<RelBuilder.AggCall> newAggCalls = new ArrayList<>();
                      int newFieldIndex = aggregate.getGroupCount();

                      for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
                          AggregateCall aggCall = aggregate.getAggCallList().get(i);
                          int originalFieldIndex = aggregate.getGroupCount() + i;

                          if (aggCall.getAggregation().getKind() == SqlKind.AVG) {
                              avgFieldMapping.put(originalFieldIndex, Pair.of(newFieldIndex, newFieldIndex + 1));

                              newAggCalls.add(
                                      builder.sum(
                                              aggCall.isDistinct(),
                                              aggCall.getName() + "_sum",
                                              builder.field(aggCall.getArgList().get(0))));
                              newAggCalls.add(
                                      builder.count(
                                              aggCall.isDistinct(),
                                              aggCall.getName() + "_count",
                                              builder.field(aggCall.getArgList().get(0))));
                              newFieldIndex += 2;
                          } else {
                              newAggCalls.add(
                                      builder
                                              .aggregateCall(
                                                      aggCall.getAggregation(),
                                                      aggCall.getArgList().stream()
                                                              .map(builder::field)
                                                              .collect(Collectors.toList()))
                                              .distinct(aggCall.isDistinct())
                                              .as(aggCall.getName()));
                              newFieldIndex++;
                          }
                      }

                      builder.aggregate(
                              builder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets()), newAggCalls);
                      return builder.build();
                  }

                  @Override
                  public RelNode visit(LogicalProject project) {
                      RelNode newInput = project.getInput().accept(this);

                      if (avgFieldMapping.isEmpty()) {
                          return project.copy(project.getTraitSet(), Collections.singletonList(newInput));
                      }

                      RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build());
                      builder.push(newInput);

                      List<RexNode> newProjects = new ArrayList<>();
                      List<String> newNames = new ArrayList<>();

                      for (int i = 0; i < project.getProjects().size(); i++) {
                          RexNode expr = project.getProjects().get(i);
                          String name = project.getRowType().getFieldNames().get(i);

                          // If this is a direct reference to an AVG field, expand to SUM + COUNT
                          if (expr instanceof RexInputRef) {
                              RexInputRef inputRef = (RexInputRef) expr;
                              Pair<Integer, Integer> mapping = avgFieldMapping.get(inputRef.getIndex());

                              if (mapping != null) {
                                  // Add both SUM and COUNT columns
                                  newProjects.add(builder.field(mapping.left));
                                  newNames.add(name + "_sum");
                                  newProjects.add(builder.field(mapping.right));
                                  newNames.add(name + "_count");
                                  continue;
                              }
                          }

                          // Keep other expressions as-is
                          newProjects.add(expr);
                          newNames.add(name);
                      }

                      builder.project(newProjects, newNames);
                      return builder.build();
                  }
              });
  }

  private static RelNode convertSpan(RelNode relNode) {
      return relNode.accept(new RelShuttleImpl() {
          @Override
          public RelNode visit(LogicalProject logicalProject) {
              List<RexNode> originalProjects = logicalProject.getProjects();
              List<RexNode> transformedProjects = new ArrayList<>();
              boolean hasSpan = false;

              for (RexNode project : originalProjects) {
                  if (isSpanFunction(project)) {
                      hasSpan = true;
                      transformedProjects.add(transformSpanToFloor(project, logicalProject.getCluster().getRexBuilder()));
                  } else {
                      transformedProjects.add(project);
                  }
              }

              if (!hasSpan) {
                  return super.visit(logicalProject);
              }

              return LogicalProject.create(
                  logicalProject.getInput(),
                  logicalProject.getHints(),
                  transformedProjects,
                  logicalProject.getRowType()
              );
          }

          private boolean isSpanFunction(RexNode node) {
              return node instanceof RexCall rexCall
                  && rexCall.getKind() == SqlKind.OTHER_FUNCTION
                  && rexCall.getOperator().getName().equalsIgnoreCase(BuiltinFunctionName.SPAN.name());
          }

          private RexNode transformSpanToFloor(RexNode spanNode, RexBuilder rexBuilder) {
              RexCall spanCall = (RexCall) spanNode;
              List<RexNode> operands = spanCall.getOperands();

              // SPAN(field, divisor, unit) -> FLOOR(field / divisor) * divisor
              if (operands.size() >= 2) {
                  RexNode field = operands.get(0);
                  RexNode divisor = operands.get(1);

                  RexNode division = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, field, divisor);
                  // Cast division to REAL for Substrait compatibility
                  RexNode realDivision = rexBuilder.makeCast(
                      rexBuilder.getTypeFactory().createSqlType(SqlTypeName.REAL),
                      division);
                  RexNode floor = rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, realDivision);
                  return rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, floor, divisor);
              } else {
                  return spanNode;
              }
          }
      });
  }

    private static RelNode convertILike(RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalFilter logicalFilter) {
                // Transform the filter condition to convert ILIKE to LIKE
                RexNode originalCondition = logicalFilter.getCondition();
                RexNode transformedCondition = transformCondition(originalCondition, logicalFilter.getCluster().getRexBuilder());

                // If no transformation occurred, return original
                if (transformedCondition == originalCondition) {
                    return super.visit(logicalFilter);
                }

                // Create new LogicalFilter with transformed condition
                return LogicalFilter.create(
                        logicalFilter.getInput(),
                        transformedCondition
                );
            }

            private RexNode transformCondition(RexNode condition, RexBuilder rexBuilder) {
                if (condition instanceof RexCall rexCall) {
                    if (isILikeFunction(rexCall)) {
                        return transformILikeToLike(rexCall, rexBuilder);
                    }

                    // Recursively transform operands for compound expressions
                    List<RexNode> originalOperands = rexCall.getOperands();
                    List<RexNode> transformedOperands = new ArrayList<>();
                    boolean hasTransformation = false;

                    for (RexNode operand : originalOperands) {
                        RexNode transformedOperand = transformCondition(operand, rexBuilder);
                        transformedOperands.add(transformedOperand);
                        if (transformedOperand != operand) {
                            hasTransformation = true;
                        }
                    }

                    // If any operand was transformed, create new call with transformed operands
                    if (hasTransformation) {
                        return rexBuilder.makeCall(rexCall.getOperator(), transformedOperands);
                    }
                }
                return condition;
            }

            private boolean isILikeFunction(RexCall rexCall) {
                return rexCall.getOperator() == SqlLibraryOperators.ILIKE;
            }

            private RexNode transformILikeToLike(RexCall iLikeCall, RexBuilder rexBuilder) {
                List<RexNode> operands = iLikeCall.getOperands();

                // ILIKE typically has 2-3 operands: (field, pattern) or (field, pattern, escape)
                if (operands.size() >= 2) {
                    RexNode field = operands.get(0);
                    RexNode pattern = operands.get(1);
                    // Use UPPER for both field and pattern so that its case in-sensitive
                    RexNode upperField = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, field);
                    RexNode upperPattern = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, pattern);

                    return rexBuilder.makeCall(SqlStdOperatorTable.LIKE, upperField, upperPattern);
                }
                return iLikeCall;
            }
        });
    }

    private static RelNode convertExtract(RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalProject logicalProject) {
                List<RexNode> originalProjects = logicalProject.getProjects();
                List<RexNode> transformedProjects = new ArrayList<>();
                boolean hasExtract = false;

                for (RexNode project : originalProjects) {
                    if (isExtractFunction(project)) {
                        hasExtract = true;
                        transformedProjects.add(transformExtract(project, logicalProject.getCluster().getRexBuilder()));
                    } else {
                        transformedProjects.add(project);
                    }
                }

                if (!hasExtract) {
                    return super.visit(logicalProject);
                }

                return LogicalProject.create(
                    logicalProject.getInput(),
                    logicalProject.getHints(),
                    transformedProjects,
                    logicalProject.getRowType()
                );
            }

            private boolean isExtractFunction(RexNode node) {
                //For UserDefinedFunctions
                return node instanceof RexCall rexCall
                        && rexCall.getKind() == SqlKind.OTHER_FUNCTION
                        && rexCall.getOperator().getName().equalsIgnoreCase("EXTRACT");
            }

            private RexNode transformExtract(RexNode extractNode, RexBuilder rexBuilder) {
                RexCall extractCall = (RexCall) extractNode;
                List<RexNode> operands = extractCall.getOperands();

                // EXTRACT has 2 operands: time unit and date field
                if (operands.size() >= 2) {
                    RexNode timeUnitOperand = operands.get(0); // The time unit (e.g., 'YEAR')
                    RexNode dateField = operands.get(1); // The date field ($0)

                    // Convert string time unit to proper TimeUnitRange flag, this is required for Substrait compatibility
                    RexNode timeUnitFlag;
                    if (timeUnitOperand instanceof org.apache.calcite.rex.RexLiteral) {
                        org.apache.calcite.rex.RexLiteral literal = (org.apache.calcite.rex.RexLiteral) timeUnitOperand;
                        String timeUnitStr = literal.getValueAs(String.class);

                        // Map the string to proper TimeUnitRange enum
                        org.apache.calcite.avatica.util.TimeUnitRange timeUnitRange = mapStringToTimeUnitRange(timeUnitStr);
                        timeUnitFlag = rexBuilder.makeFlag(timeUnitRange);
                    } else {
                        // If not a literal, use as-is (fallback)
                        timeUnitFlag = timeUnitOperand;
                    }

                    // Create the standard EXTRACT call using SqlStdOperatorTable.EXTRACT
                    // This maintains the same semantic meaning but uses the standard operator
                    return rexBuilder.makeCall(
                        SqlStdOperatorTable.EXTRACT,
                        timeUnitFlag,
                        dateField
                    );
                } else {
                    return extractNode;
                }
            }

            // TODO: Support all the formats given in https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/functions/datetime.rst#extract
            private org.apache.calcite.avatica.util.TimeUnitRange mapStringToTimeUnitRange(String timeUnitStr) {
                // Map OpenSearch time unit strings to Calcite TimeUnitRange
                switch (timeUnitStr.toUpperCase()) {
                    case "YEAR_MONTH":
                        return org.apache.calcite.avatica.util.TimeUnitRange.YEAR_TO_MONTH;
                    case "YEAR":
                        return org.apache.calcite.avatica.util.TimeUnitRange.YEAR;
                    case "MONTH":
                        return org.apache.calcite.avatica.util.TimeUnitRange.MONTH;
                    case "DAY":
                        return org.apache.calcite.avatica.util.TimeUnitRange.DAY;
                    case "HOUR":
                        return org.apache.calcite.avatica.util.TimeUnitRange.HOUR;
                    case "MINUTE":
                        return org.apache.calcite.avatica.util.TimeUnitRange.MINUTE;
                    case "SECOND":
                        return org.apache.calcite.avatica.util.TimeUnitRange.SECOND;
                    case "QUARTER":
                        return org.apache.calcite.avatica.util.TimeUnitRange.QUARTER;
                    case "WEEK":
                        return org.apache.calcite.avatica.util.TimeUnitRange.WEEK;
                    case "MICROSECOND":
                        return org.apache.calcite.avatica.util.TimeUnitRange.MICROSECOND;
                    case "DAY_HOUR":
                        return org.apache.calcite.avatica.util.TimeUnitRange.DAY_TO_HOUR;
                    case "DAY_MINUTE":
                        return org.apache.calcite.avatica.util.TimeUnitRange.DAY_TO_MINUTE;
                    case "DAY_SECOND":
                        return org.apache.calcite.avatica.util.TimeUnitRange.DAY_TO_SECOND;
                    case "HOUR_MINUTE":
                        return org.apache.calcite.avatica.util.TimeUnitRange.HOUR_TO_MINUTE;
                    case "HOUR_SECOND":
                        return org.apache.calcite.avatica.util.TimeUnitRange.HOUR_TO_SECOND;
                    case "MINUTE_SECOND":
                        return org.apache.calcite.avatica.util.TimeUnitRange.MINUTE_TO_SECOND;
                    default:
                        // Default fallback to YEAR if unknown
                        return org.apache.calcite.avatica.util.TimeUnitRange.YEAR;
                }
            }
        });
    }

    private static RexNode updateTimeStampFunction(RexNode rexNode, org.apache.calcite.rel.type.RelDataType inputRowType, RexBuilder rexBuilder) {
        if(rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            List<RexNode> originalOperands = rexCall.getOperands();
            List<RexNode> updatedOperands = new ArrayList<>();
            for (RexNode operand : originalOperands) {
                if(operand instanceof RexCall timestampCall && isTimestampFunction((RexCall) operand)) {
                    org.apache.calcite.rel.type.RelDataType timestampType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3);
                    updatedOperands.add(rexBuilder.makeCall(timestampCall.pos, timestampType, SAFE_CAST, timestampCall.getOperands()));
                } else if(operand instanceof RexInputRef timeStampInput) {
                    org.apache.calcite.rel.type.RelDataType timestampType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3);
                    updatedOperands.add(rexBuilder.makeInputRef(timestampType,  timeStampInput.getIndex()));
                } else {
                    updatedOperands.add(operand);
                }
            }
            return rexBuilder.makeCall(rexCall.pos, rexCall.type, rexCall.getOperator(), updatedOperands);
        }
        return rexNode;
    }

    private static boolean isTimestampFunction(RexCall rexCall) {
        return rexCall.getOperator().getName().equalsIgnoreCase("TIMESTAMP");
    }

    private static RelNode convertTimestamp(RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {

            @Override
            public RelNode visit(LogicalFilter logicalFilter) {
                RelNode newInput = logicalFilter.getInput().accept(this);
                RexNode originalCondition = logicalFilter.getCondition();
                RexNode updatedCondition = updateTimeStampFunction(originalCondition, newInput.getRowType(), logicalFilter.getCluster().getRexBuilder());
                return LogicalFilter.create(newInput, updatedCondition);
            }
        });
    }

    private static RelNode ensureCountAggregate(RelNode relNode) {
        return relNode.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit(LogicalAggregate aggregate) {
                        boolean hasCount =
                                aggregate.getAggCallList().stream()
                                        .anyMatch(call -> call.getAggregation().getKind() == SqlKind.COUNT);

                        if (hasCount) {
                            return super.visit(aggregate);
                        }

                        RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build());
                        builder.push(aggregate.getInput());

                        List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
                        for (AggregateCall call : aggregate.getAggCallList()) {
                            aggCalls.add(
                                    builder
                                            .aggregateCall(
                                                    call.getAggregation(),
                                                    call.getArgList().stream()
                                                            .map(builder::field)
                                                            .collect(Collectors.toList()))
                                            .distinct(call.isDistinct())
                                            .as(call.getName()));
                        }
                        aggCalls.add(builder.count(false, INJECTED_COUNT_AGGREGATE_NAME));

                        builder.aggregate(builder.groupKey(aggregate.getGroupSet()), aggCalls);
                        return builder.build();
                    }

                    @Override
                    public RelNode visit(LogicalProject project) {
                        RelNode input = project.getInput().accept(this);
                        if (input == project.getInput()) {
                            return project;
                        }

                        if (input instanceof LogicalAggregate agg) {
                            int countIndex = agg.getGroupCount() + agg.getAggCallList().size() - 1;
                            RexBuilder rexBuilder = project.getCluster().getRexBuilder();

                            List<RexNode> newProjects = new ArrayList<>(project.getProjects());
                            newProjects.add(
                                    rexBuilder.makeInputRef(
                                            agg.getRowType().getFieldList().get(countIndex).getType(), countIndex));

                            List<String> newNames = new ArrayList<>(project.getRowType().getFieldNames());
                            newNames.add(INJECTED_COUNT_AGGREGATE_NAME);

                            return LogicalProject.create(input, project.getHints(), newProjects, newNames);
                        }

                        return project;
                    }
                });
    }

}
