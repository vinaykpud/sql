/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.action.search.*;
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
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

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

  /** {@link OpenSearchRequest.IndexName}. */
  private final IndexName indexName;

  /** Search request source builder. */
  private SearchSourceBuilder sourceBuilder;

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
        return new OpenSearchResponse(SearchHits.empty(), exprValueFactory, includes);
      } else {
        searchDone = true;
        sourceBuilder.queryPlanIR(convertToSubstraitAndSerialize());
        return new OpenSearchResponse(
            searchAction.apply(
                new SearchRequest().indices(indexName.getIndexNames()).source(sourceBuilder)),
            exprValueFactory,
            includes);
      }
    } else {
      // Search with PIT instead of scroll API
      return searchWithPIT(searchAction);
    }
  }

  public OpenSearchResponse searchWithPIT(Function<SearchRequest, SearchResponse> searchAction) {
    OpenSearchResponse openSearchResponse;
    if (searchDone) {
      openSearchResponse = new OpenSearchResponse(SearchHits.empty(), exprValueFactory, includes);
    } else {
      this.sourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(this.pitId));
      this.sourceBuilder.timeout(cursorKeepAlive);
      this.sourceBuilder.queryPlanIR(convertToSubstraitAndSerialize());
      // check for search after
      if (searchAfter != null) {
        this.sourceBuilder.searchAfter(searchAfter);
      }
      // Set sort field for search_after
      if (this.sourceBuilder.sorts() == null) {
        this.sourceBuilder.sort(DOC_FIELD_NAME, ASC);
        // Workaround to preserve sort location more exactly,
        // see https://github.com/opensearch-project/sql/pull/3061
        this.sourceBuilder.sort(METADATA_FIELD_ID, ASC);
      }
      SearchRequest searchRequest =
          new SearchRequest().indices(indexName.getIndexNames()).source(this.sourceBuilder);
      this.searchResponse = searchAction.apply(searchRequest);

      openSearchResponse = new OpenSearchResponse(this.searchResponse, exprValueFactory, includes);

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

    public static byte[] convertToSubstraitAndSerialize() {
        RelNode relNode = CalciteToolsHelper.OpenSearchRelRunners.getCurrentRelNode();
      // Generate unique filename using epoch time
        // Step 1: Load default Substrait extensions
        // This includes standard functions, operators, and data types needed for conversion
        SimpleExtension.ExtensionCollection EXTENSIONS = SimpleExtension.loadDefaults();

        // Step 2: Wrap RelNode in a RelRoot with query kind
        // RelRoot represents the root of a relational query tree with metadata
        // SqlKind.SELECT indicates this is a SELECT query (vs INSERT, UPDATE, etc.)
        RelRoot root = RelRoot.of(relNode, SqlKind.SELECT);

        // Step 3: Convert Calcite RelRoot to Substrait Plan.Root
        // This is the core conversion step using SubstraitRelVisitor
        // The visitor traverses the Calcite tree and converts each node to Substrait equivalent
        // TODO: Explore better way to do this visiting, how to pass UDTs
        Plan.Root substraitRoot = SubstraitRelVisitor.convert(root, EXTENSIONS);

        // Step 4: Build the complete Substrait Plan
        // Plan contains one or more roots (query entry points) and shared extensions
        // addRoots() adds the converted relation tree as a query root
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        // Step 5: Plan now contains two table names like bellow
        //        named_table {
        //            names: "OpenSearch"
        //            names: "hits"
        //        }
        // we want to remove "OpenSearch" as table name for now otherwise execution fails in DF
        TableNameModifier modifier = new TableNameModifier();
        Plan modifiedPlan = modifier.modifyTableNames(plan);

        // Step 6: Convert to Protocol Buffer format for serialization
        // PlanProtoConverter handles the conversion from Java objects to protobuf
        // This enables serialization, storage, and cross-system communication
        PlanProtoConverter planProtoConverter = new PlanProtoConverter();
        io.substrait.proto.Plan substraitPlanProto = planProtoConverter.toProto(plan);
        io.substrait.proto.Plan substraitPlanProtoModified = planProtoConverter.toProto(modifiedPlan);
        return substraitPlanProtoModified.toByteArray();
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
}
