/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FromClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.HavingClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SelectClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SelectElementContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SubqueryAsRelationContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableAsRelationContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableFunctionRelationContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.WhereClauseContext;
import static org.opensearch.sql.sql.parser.ParserUtils.getTextInQuery;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QuerySpecificationContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.sql.parser.context.ParsingContext;

/** Abstract syntax tree (AST) builder. */
public class AstBuilder extends OpenSearchSQLParserBaseVisitor<UnresolvedPlan> {

  private final AstExpressionBuilder expressionBuilder;

  /** Parsing context stack that contains context for current query parsing. */
  private final ParsingContext context = new ParsingContext();

  /**
   * SQL query to get original token text. This is necessary because token.getText() returns text
   * without whitespaces or other characters discarded by lexer.
   */
  private final String query;

  public AstBuilder(String query) {
    this.query = query;
    this.expressionBuilder = createExpressionBuilder();
  }

  @Override
  public UnresolvedPlan visitShowStatement(OpenSearchSQLParser.ShowStatementContext ctx) {
    final UnresolvedExpression tableFilter = visitAstExpression(ctx.tableFilter());
    return new Project(Collections.singletonList(AllFields.of()))
        .attach(new Filter(tableFilter).attach(new DescribeRelation(qualifiedName(TABLE_INFO))));
  }

  @Override
  public UnresolvedPlan visitDescribeStatement(OpenSearchSQLParser.DescribeStatementContext ctx) {
    final Function tableFilter = (Function) visitAstExpression(ctx.tableFilter());
    final String tableName = tableFilter.getFuncArgs().get(1).toString();
    final Relation table = new DescribeRelation(qualifiedName(mappingTable(tableName.toString())));
    if (ctx.columnFilter() == null) {
      return new Project(Collections.singletonList(AllFields.of())).attach(table);
    } else {
      return new Project(Collections.singletonList(AllFields.of()))
          .attach(new Filter(visitAstExpression(ctx.columnFilter())).attach(table));
    }
  }

  @Override
  public UnresolvedPlan visitQuerySpecification(QuerySpecificationContext queryContext) {
    context.push();
    context.peek().collect(queryContext, query);

    Project project = (Project) visit(queryContext.selectClause());

    if (queryContext.fromClause() == null) {
      Optional<UnresolvedExpression> allFields =
          project.getProjectList().stream().filter(node -> node instanceof AllFields).findFirst();
      if (allFields.isPresent()) {
        throw new SyntaxCheckException("No FROM clause found for select all");
      }
      // Attach an Values operator with only a empty row inside so that
      // Project operator can have a chance to evaluate its expression
      // though the evaluation doesn't have any dependency on what's in Values.
      Values emptyValue = new Values(ImmutableList.of(emptyList()));
      return project.attach(emptyValue);
    }

    // If limit (and offset) keyword exists:
    // Add Limit node, plan structure becomes:
    // Project -> Limit -> visit(fromClause)
    // Else:
    // Project -> visit(fromClause)
    //
    // Exception: when the SELECT list contains a window function (e.g.
    // ROW_NUMBER() OVER (...)), the Limit must sit ABOVE the Project so the
    // window sees the full input. Otherwise Calcite resolves the AST to
    // LogicalProject(RexOver) -> LogicalSort(fetch=N) and the window only
    // ranks N pre-window rows. Neither the SQL HEP planner nor the
    // analytics-engine planner enables SortProjectTransposeRule, so the AST
    // shape we emit here is what reaches the optimizer.
    UnresolvedPlan from = visit(queryContext.fromClause());
    if (projectListHasWindowFunction(project)) {
      // Window-aware shape: ORDER BY (if any) and LIMIT must sit ABOVE the Project so the
      // window ranks the full input AND so `ORDER BY <window-alias>` doesn't get rewritten
      // to a duplicate RexOver below the Project. visitFromClause already skipped attaching
      // the Sort (see hasWindowFunctionInProjectList branch there); we re-attach it here.
      UnresolvedPlan result = project.attach(from);
      OpenSearchSQLParser.OrderByClauseContext orderByCtx =
          queryContext.fromClause() != null ? queryContext.fromClause().orderByClause() : null;
      if (orderByCtx != null) {
        AstSortBuilder sortBuilder = new AstSortBuilder(context.peek());
        result = ((Sort) sortBuilder.visit(orderByCtx)).attach(result);
      }
      if (queryContext.limitClause() != null) {
        result = visit(queryContext.limitClause()).attach(result);
      }
      context.pop();
      return result;
    }
    if (queryContext.limitClause() != null) {
      from = visit(queryContext.limitClause()).attach(from);
    }
    UnresolvedPlan result = project.attach(from);
    context.pop();
    return result;
  }

  /**
   * True when any expression in {@code project}'s SELECT list is (or directly aliases) a
   * {@link WindowFunction}. Used to route Limit above Project so window functions rank over
   * the full input rather than the first N rows.
   */
  private static boolean projectListHasWindowFunction(Project project) {
    for (UnresolvedExpression expr : project.getProjectList()) {
      UnresolvedExpression target = (expr instanceof Alias) ? ((Alias) expr).getDelegated() : expr;
      if (target instanceof WindowFunction) {
        return true;
      }
    }
    return false;
  }

  @Override
  public UnresolvedPlan visitSelectClause(SelectClauseContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = new ImmutableList.Builder<>();
    if (ctx.selectElements().star != null) { // TODO: project operator should be required?
      builder.add(AllFields.of());
    }
    ctx.selectElements().selectElement().forEach(field -> builder.add(visitSelectItem(field)));
    return new Project(builder.build());
  }

  @Override
  public UnresolvedPlan visitLimitClause(OpenSearchSQLParser.LimitClauseContext ctx) {
    return new Limit(
        Integer.parseInt(ctx.limit.getText()),
        ctx.offset == null ? 0 : Integer.parseInt(ctx.offset.getText()));
  }

  @Override
  public UnresolvedPlan visitFromClause(FromClauseContext ctx) {
    UnresolvedPlan result = visit(ctx.relation());

    for (var joinCtx : ctx.joinClause()) {
      result = visit(joinCtx).attach(result);
    }

    if (ctx.whereClause() != null) {
      result = visit(ctx.whereClause()).attach(result);
    }

    // Because aggregation maybe implicit, this has to be handled here instead of visitGroupByClause
    AstAggregationBuilder aggBuilder = new AstAggregationBuilder(context.peek());
    UnresolvedPlan aggregation = aggBuilder.visit(ctx.groupByClause());
    if (aggregation != null) {
      result = aggregation.attach(result);
    }

    if (ctx.havingClause() != null) {
      UnresolvedPlan havingPlan = visit(ctx.havingClause());
      verifySupportsCondition(((Filter) havingPlan).getCondition());
      result = visit(ctx.havingClause()).attach(result);
    }

    if (ctx.orderByClause() != null && !context.peek().hasWindowFunctionInProjectList()) {
      // Window-aware path: if a window function appears in the SELECT list, defer ORDER BY
      // attachment to visitQuerySpecification so the Sort can sit ABOVE the Project. Attaching
      // it here would put the Sort below the user's Project; replaceIfAliasOrOrdinal then
      // expands `ORDER BY <window-alias>` into the underlying RexOver, and Calcite splits the
      // plan into Project(RexOver) -> Sort(RexOver) -> Project(RexOver) — two RexOvers reach
      // the reduce-stage substrait emitter and DataFusion sees duplicate unqualified field
      // names ("row_number() ORDER BY [...] RANGE BETWEEN ...").
      AstSortBuilder sortBuilder = new AstSortBuilder(context.peek());
      result = sortBuilder.visit(ctx.orderByClause()).attach(result);
    }
    return result;
  }

  /**
   * Ensure NESTED function is not used in HAVING clause and fallback to legacy engine. Can remove
   * when support is added for NESTED function in HAVING clause.
   *
   * @param func : Function in HAVING clause
   */
  private void verifySupportsCondition(UnresolvedExpression func) {
    if (func instanceof Function) {
      if (((Function) func).getFuncName().equalsIgnoreCase(BuiltinFunctionName.NESTED.name())) {
        throw new SyntaxCheckException(
            "Falling back to legacy engine. Nested function is not supported in the HAVING"
                + " clause.");
      }
      ((Function) func).getFuncArgs().stream().forEach(e -> verifySupportsCondition(e));
    }
  }

  @Override
  public UnresolvedPlan visitTableAsRelation(TableAsRelationContext ctx) {
    Relation relation = new Relation(visitAstExpression(ctx.tableName()));
    return ctx.alias() != null
        ? new SubqueryAlias(StringUtils.unquoteIdentifier(ctx.alias().getText()), relation)
        : relation;
  }

  @Override
  public UnresolvedPlan visitSubqueryAsRelation(SubqueryAsRelationContext ctx) {
    String subqueryAlias = StringUtils.unquoteIdentifier(ctx.alias().getText());
    return new RelationSubquery(visit(ctx.subquery), subqueryAlias);
  }

  @Override
  public UnresolvedPlan visitTableFunctionRelation(TableFunctionRelationContext ctx) {
    // The grammar accepts both `ident = value` and bare `value` forms for each table function
    // argument so that the real positional shape (e.g. `vectorSearch('idx', field='f', ...)`)
    // reaches this V2 builder instead of failing to parse and silently falling back to the
    // legacy SQL engine. Reject the positional shape here with a SemanticCheckException so the
    // user receives a clean 400 rather than an opaque legacy parser error.
    ctx.tableFunctionArgs()
        .tableFunctionArg()
        .forEach(
            arg -> {
              if (arg.ident() == null) {
                String functionName = ctx.qualifiedName().getText();
                throw new SemanticCheckException(
                    String.format(
                        Locale.ROOT,
                        "Table function '%s' requires named arguments (e.g. name='value'),"
                            + " but received a positional argument: %s",
                        functionName,
                        arg.functionArg().getText()));
              }
            });
    ImmutableList.Builder<UnresolvedExpression> args = ImmutableList.builder();
    ctx.tableFunctionArgs()
        .tableFunctionArg()
        .forEach(
            arg -> {
              String argName =
                  StringUtils.unquoteIdentifier(arg.ident().getText()).toLowerCase(Locale.ROOT);
              UnresolvedExpression argValue = visitAstExpression(arg.functionArg());
              args.add(new UnresolvedArgument(argName, argValue));
            });
    TableFunction tableFunction =
        new TableFunction(visitAstExpression(ctx.qualifiedName()), args.build());
    if (ctx.alias() == null) {
      String functionName = ctx.qualifiedName().getText();
      // Use SemanticCheckException (not SyntaxCheckException) so the request does not fall back
      // to the legacy SQL engine, whose opaque parser error would mask this message.
      throw new SemanticCheckException(
          String.format(
              Locale.ROOT,
              "Table function '%s' requires a table alias."
                  + " Add an alias after the closing parenthesis, for example:"
                  + " FROM %s(...) AS v",
              functionName,
              functionName));
    }
    String alias = StringUtils.unquoteIdentifier(ctx.alias().getText());
    return new SubqueryAlias(alias, tableFunction);
  }

  @Override
  public UnresolvedPlan visitWhereClause(WhereClauseContext ctx) {
    return new Filter(visitAstExpression(ctx.expression()));
  }

  @Override
  public UnresolvedPlan visitJoinClause(OpenSearchSQLParser.JoinClauseContext ctx) {
    throw new SyntaxCheckException(
        "JOIN is not supported in the V2 SQL engine. Falling back to legacy engine.");
  }

  @Override
  public UnresolvedPlan visitUnionSelect(OpenSearchSQLParser.UnionSelectContext ctx) {
    throw new SyntaxCheckException(
        "UNION is not supported in the V2 SQL engine. Falling back to legacy engine.");
  }

  @Override
  public UnresolvedPlan visitHavingClause(HavingClauseContext ctx) {
    AstHavingFilterBuilder builder = new AstHavingFilterBuilder(context.peek());
    return new Filter(builder.visit(ctx.expression()));
  }

  @Override
  protected UnresolvedPlan aggregateResult(UnresolvedPlan aggregate, UnresolvedPlan nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  /**
   * Visit expression tree node and convert to UnresolvedExpression. Protected to allow subclass
   * access (e.g., ExtendedAstBuilder for join conditions).
   */
  protected UnresolvedExpression visitAstExpression(ParseTree tree) {
    return expressionBuilder.visit(tree);
  }

  /** Override to provide a custom expression builder (e.g., with subquery support). */
  protected AstExpressionBuilder createExpressionBuilder() {
    return new AstExpressionBuilder();
  }

  private UnresolvedExpression visitSelectItem(SelectElementContext ctx) {
    String name = StringUtils.unquoteIdentifier(getTextInQuery(ctx.expression(), query));
    UnresolvedExpression expr = visitAstExpression(ctx.expression());

    if (ctx.alias() == null) {
      return Alias.newAliasAllowMetaMetaField(name, expr, null);
    } else {
      String alias = StringUtils.unquoteIdentifier(ctx.alias().getText());
      return Alias.newAliasAllowMetaMetaField(name, expr, alias);
    }
  }
}
