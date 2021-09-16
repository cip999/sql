/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter;

import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;
import static java.util.Collections.emptyMap;
import static org.apache.lucene.search.join.ScoreMode.None;
import static org.elasticsearch.script.Script.DEFAULT_SCRIPT_TYPE;

import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter.lucene.LuceneQuery;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter.lucene.RangeQuery;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter.lucene.RangeQuery.Comparison;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter.lucene.TermQuery;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter.lucene.WildcardQuery;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.serialization.ExpressionSerializer;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.ExpressionNodeVisitor;
import com.amazon.opendistroforelasticsearch.sql.expression.FunctionExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.NestedExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionName;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Getter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

public class FilterQueryBuilder extends ExpressionNodeVisitor<QueryBuilder, Object> {

  /**
   * Serializer that serializes expression for build DSL query.
   */
  private final ExpressionSerializer serializer;

  /**
   * Query projects.
   */
  @Getter
  private Set<ReferenceExpression> projectList;

  /**
   * Mapping from function name to lucene query builder.
   */
  private final Map<FunctionName, LuceneQuery> luceneQueries =
      ImmutableMap.<FunctionName, LuceneQuery>builder()
          .put(BuiltinFunctionName.EQUAL.getName(), new TermQuery())
          .put(BuiltinFunctionName.LESS.getName(), new RangeQuery(Comparison.LT))
          .put(BuiltinFunctionName.GREATER.getName(), new RangeQuery(Comparison.GT))
          .put(BuiltinFunctionName.LTE.getName(), new RangeQuery(Comparison.LTE))
          .put(BuiltinFunctionName.GTE.getName(), new RangeQuery(Comparison.GTE))
          .put(BuiltinFunctionName.LIKE.getName(), new WildcardQuery())
          .build();

  public FilterQueryBuilder(ExpressionSerializer serializer) {
    this(serializer, new HashSet<>());
  }

  public FilterQueryBuilder(ExpressionSerializer serializer, Set<ReferenceExpression> projectList) {
    this.serializer = serializer;
    this.projectList = projectList;
  }

  /**
   * Build Elasticsearch filter query from expression.
   * @param expr  expression
   * @return      query
   */
  public QueryBuilder build(Expression expr) {
    return expr.accept(this, null);
  }

  @Override
  public QueryBuilder visitFunction(FunctionExpression func, Object context) {
    FunctionName name = func.getFunctionName();
    QueryBuilder queryBuilder;

    switch (name.getFunctionName()) {
      case "and":
        queryBuilder = buildBoolQuery(func, context, BoolQueryBuilder::filter);
        break;
      case "or":
        queryBuilder = buildBoolQuery(func, context, BoolQueryBuilder::should);
        break;
      case "not":
        queryBuilder = buildBoolQuery(func, context, BoolQueryBuilder::mustNot);
        break;
      default: {
        LuceneQuery query = luceneQueries.get(name);
        if (query != null && query.canSupport(func)) {
          queryBuilder = query.build(func);
        } else {
          queryBuilder = buildScriptQuery(func);
        }
      }
    }

    String nestedPath = null;
    String[] includes = new String[0];

    if (queryBuilder instanceof BoolQueryBuilder) {
      BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
      Set<String> nestedPaths = getNestedPaths(boolQueryBuilder);
      if (nestedPaths.size() == 1 && areAllClausesNested(boolQueryBuilder)) {
        queryBuilder = unwrapNestedBuilders(boolQueryBuilder);
        nestedPath = (String) nestedPaths.toArray()[0];
        includes = getIncludes(boolQueryBuilder);
      }
    } else {
      Optional<NestedExpression> nestedExpr = func.getArguments()
              .stream()
              .filter(arg -> arg instanceof NestedExpression)
              .map(arg -> (NestedExpression) arg)
              .findAny();
      if (nestedExpr.isPresent()) {
        nestedPath = nestedExpr.get().getNestedPath();
        final String finalNestedPath = nestedPath;
        List<ReferenceExpression> nestedArgs = projectList.stream()
                .filter(project -> (project instanceof NestedExpression
                        && ((NestedExpression) project).getNestedPath().equals(finalNestedPath)))
                .collect(Collectors.toList());
        projectList.removeAll(nestedArgs);
        includes = nestedArgs.stream().map(ReferenceExpression::getAttr).toArray(String[]::new);
      }
    }

    if (nestedPath != null) {
      if (includes.length == 0) {
        return new NestedQueryBuilder(nestedPath, queryBuilder, None);
      } else {
        return new NestedQueryBuilder(nestedPath, queryBuilder, None).innerHit(
                new InnerHitBuilder().setName(nestedPath).setFetchSourceContext(
                        new FetchSourceContext(true, includes, new String[0])
                )
        );
      }
    } else {
      return queryBuilder;
    }
  }

  private boolean areAllClausesNested(BoolQueryBuilder queryBuilder) {
    for (List<QueryBuilder> clauses : Stream.of(new ArrayList<>(Arrays.asList(
            queryBuilder.must(),
            queryBuilder.mustNot(),
            queryBuilder.filter(),
            queryBuilder.should())))
            .flatMap(Collection::stream)
            .collect(Collectors.toList())) {
      if (clauses.stream().anyMatch(qb -> !(qb instanceof NestedQueryBuilder))) {
        return false;
      }
    }
    return true;
  }

  private Set<String> getNestedPaths(BoolQueryBuilder queryBuilder) {
    Set<String> nestedPaths = new HashSet<>();
    for (List<QueryBuilder> clauses : Stream.of(new ArrayList<>(Arrays.asList(
            queryBuilder.must(),
            queryBuilder.mustNot(),
            queryBuilder.filter(),
            queryBuilder.should())))
            .flatMap(Collection::stream)
            .collect(Collectors.toList())) {
      nestedPaths.addAll(clauses.stream()
              .filter(qb -> qb instanceof NestedQueryBuilder
                      && ((NestedQueryBuilder) qb).innerHit() != null)
              .map(qb -> ((NestedQueryBuilder) qb).innerHit().getName())
              .collect(Collectors.toList()));
    }
    return nestedPaths;
  }

  private String[] getIncludes(BoolQueryBuilder queryBuilder) {
    Set<String> includes = new HashSet<>();
    for (List<QueryBuilder> clauses : Stream.of(new ArrayList<>(Arrays.asList(
            queryBuilder.must(),
            queryBuilder.mustNot(),
            queryBuilder.filter(),
            queryBuilder.should())))
            .flatMap(Collection::stream)
            .collect(Collectors.toList())) {
      includes.addAll(clauses.stream()
              .filter(qb -> qb instanceof NestedQueryBuilder
                      && ((NestedQueryBuilder) qb).innerHit() != null)
              .flatMap(qb -> Arrays.stream(((NestedQueryBuilder) qb)
                      .innerHit().getFetchSourceContext().includes()))
              .collect(Collectors.toList()));
    }
    return includes.toArray(new String[0]);
  }

  private QueryBuilder unwrapNestedBuilders(BoolQueryBuilder queryBuilder) {
    BoolQueryBuilder newQueryBuilder = new BoolQueryBuilder();
    addUnwrappedClauses(newQueryBuilder, queryBuilder.must(), BoolQueryBuilder::must);
    addUnwrappedClauses(newQueryBuilder, queryBuilder.mustNot(), BoolQueryBuilder::mustNot);
    addUnwrappedClauses(newQueryBuilder, queryBuilder.filter(), BoolQueryBuilder::filter);
    addUnwrappedClauses(newQueryBuilder, queryBuilder.should(), BoolQueryBuilder::should);
    return newQueryBuilder;
  }

  private void addUnwrappedClauses(BoolQueryBuilder queryBuilder, List<QueryBuilder> clauses,
                                   BiFunction<BoolQueryBuilder,
                                           QueryBuilder,
                                           QueryBuilder> accumulator) {
    clauses.forEach(clause -> accumulator.apply(queryBuilder,
            clause instanceof NestedQueryBuilder
                    ? ((NestedQueryBuilder) clause).query()
                    : clause
    ));
  }

  private BoolQueryBuilder buildBoolQuery(FunctionExpression node,
                                          Object context,
                                          BiFunction<BoolQueryBuilder, QueryBuilder,
                                              QueryBuilder> accumulator) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    for (Expression arg : node.getArguments()) {
      accumulator.apply(boolQuery, arg.accept(this, context));
    }
    return boolQuery;
  }

  private ScriptQueryBuilder buildScriptQuery(FunctionExpression node) {
    return new ScriptQueryBuilder(new Script(
        DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(node), emptyMap()));
  }

}
