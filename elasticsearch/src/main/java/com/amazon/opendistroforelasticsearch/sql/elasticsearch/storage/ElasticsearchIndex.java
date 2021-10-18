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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage;

import static org.apache.lucene.search.join.ScoreMode.None;

import com.amazon.opendistroforelasticsearch.sql.common.antlr.SyntaxCheckException;
import com.amazon.opendistroforelasticsearch.sql.common.setting.Settings;
import com.amazon.opendistroforelasticsearch.sql.common.utils.StringUtils;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.ElasticsearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value.ElasticsearchExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.planner.logical.ElasticsearchLogicalIndexAgg;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.planner.logical.ElasticsearchLogicalIndexScan;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.planner.logical.ElasticsearchLogicalPlanOptimizerFactory;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.system.ElasticsearchDescribeIndexRequest;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.aggregation.AggregationQueryBuilder;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter.FilterQueryBuilder;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.sort.SortQueryBuilder;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.serialization.DefaultExpressionSerializer;
import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.amazon.opendistroforelasticsearch.sql.expression.NestedExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.planner.DefaultImplementor;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalRelation;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import com.amazon.opendistroforelasticsearch.sql.storage.Table;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

/** Elasticsearch table (index) implementation. */
@RequiredArgsConstructor
public class ElasticsearchIndex implements Table {

  /** Elasticsearch client connection. */
  private final ElasticsearchClient client;

  private final Settings settings;

  /** Current Elasticsearch index name. */
  private final String indexName;

  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      cachedFieldTypes = new ElasticsearchDescribeIndexRequest(client, indexName).getFieldTypes();
    }
    return cachedFieldTypes;
  }

  /**
   * TODO: Push down operations to index scan operator as much as possible in future.
   */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    ElasticsearchIndexScan indexScan = new ElasticsearchIndexScan(client, settings, indexName,
        new ElasticsearchExprValueFactory(getFieldTypes(),
                !plan.getChild().isEmpty()
                        && plan.getChild().get(0) instanceof ElasticsearchLogicalIndexScan
                        ? ((ElasticsearchLogicalIndexScan) plan.getChild().get(0)).getLimit()
                        : null));

    /*
     * Visit logical plan with index scan as context so logical operators visited, such as
     * aggregation, filter, will accumulate (push down) Elasticsearch query and aggregation DSL on
     * index scan.
     */
    return plan.accept(new ElasticsearchDefaultImplementor(indexScan), indexScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return ElasticsearchLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class ElasticsearchDefaultImplementor
      extends DefaultImplementor<ElasticsearchIndexScan> {
    private final ElasticsearchIndexScan indexScan;

    @Override
    public PhysicalPlan visitNode(LogicalPlan plan, ElasticsearchIndexScan context) {
      if (plan instanceof ElasticsearchLogicalIndexScan) {
        return visitIndexScan((ElasticsearchLogicalIndexScan) plan, context);
      } else if (plan instanceof ElasticsearchLogicalIndexAgg) {
        return visitIndexAggregation((ElasticsearchLogicalIndexAgg) plan, context);
      } else {
        throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
            plan.getClass()));
      }
    }

    /**
     * Implement ElasticsearchLogicalIndexScan.
     */
    public PhysicalPlan visitIndexScan(ElasticsearchLogicalIndexScan node,
                                       ElasticsearchIndexScan context) {
      if (null != node.getSortList()) {
        final SortQueryBuilder builder = new SortQueryBuilder();
        context.pushDownSort(node.getSortList().stream()
            .map(sort -> builder.build(sort.getValue(), sort.getKey()))
            .collect(Collectors.toList()));
      }

      QueryBuilder query;
      Set<ReferenceExpression> projectList = (node.getProjectList() != null
              ? new HashSet<>(node.getProjectList()) : new HashSet<>());
      Set<ReferenceExpression> unconsumedProjects;
      if (null == node.getFilter()) {
        query = new MatchAllQueryBuilder();
        unconsumedProjects = projectList.stream()
                .filter(project -> project instanceof NestedExpression)
                .collect(Collectors.toSet());
      } else {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
                new DefaultExpressionSerializer(), projectList
        );
        query = queryBuilder.build(node.getFilter());
        final Map<String, Integer> innerHitsCount = queryBuilder.getInnerHitsCount();
        if (innerHitsCount.values().stream().anyMatch(c -> c > 1)) {
          throw new ExpressionEvaluationException(
                  "Cannot have more than one occurrence of the same nested path in WHERE statement"
          );
        }
        unconsumedProjects = projectList.stream()
                .filter(project -> project instanceof NestedExpression)
                .map(project -> (NestedExpression) project)
                .filter(project -> !innerHitsCount.containsKey(project.getNestedPath()))
                .collect(Collectors.toSet());
      }
      query = includeUnconsumedProjects(query, unconsumedProjects);
      query = expandMustNotClauses(query);
      context.pushDown(query);

      if (node.getLimit() != null) {
        context.pushDownLimit(node.getLimit(), node.getOffset());
      }

      if (node.hasProjects()) {
        context.pushDownProjects(node.getProjectList());
      }

      return indexScan;
    }

    private QueryBuilder includeUnconsumedProjects(QueryBuilder query,
                                                   Set<ReferenceExpression> projects) {
      BoolQueryBuilder newQuery = new BoolQueryBuilder().must(query);
      Map<String, Set<String>> projectsByPath = new HashMap<>();

      List<NestedExpression> nestedProjects = projects.stream()
              .filter(project -> project instanceof NestedExpression)
              .map(project -> (NestedExpression) project)
              .collect(Collectors.toList());
      for (NestedExpression project : nestedProjects) {
        projectsByPath.putIfAbsent(project.getNestedPath(), new HashSet<>());
        projectsByPath.get(project.getNestedPath()).add(project.getAttr());
      }

      for (String nestedPath : projectsByPath.keySet()) {
        newQuery.must(new NestedQueryBuilder(nestedPath, new MatchAllQueryBuilder(), None)
                .innerHit(new InnerHitBuilder(nestedPath)
                        .setFetchSourceContext(new FetchSourceContext(true,
                                projectsByPath.get(nestedPath).toArray(new String[0]),
                                new String[0])
                        )
                )
        );
      }

      return newQuery;
    }

    private QueryBuilder expandMustNotClauses(QueryBuilder query) {
      if (query instanceof NestedQueryBuilder) {
        NestedQueryBuilder nestedQuery = (NestedQueryBuilder) query;
        return new NestedQueryBuilder(
                nestedQuery.innerHit().getName(),
                expandMustNotClauses(nestedQuery.query()),
                None
        ).innerHit(nestedQuery.innerHit());
      } else if (!(query instanceof BoolQueryBuilder)) {
        return query;
      }

      BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
      final BoolQueryBuilder newQuery = new BoolQueryBuilder();

      boolQuery.must().forEach(c -> newQuery.must(expandMustNotClauses(c)));
      boolQuery.filter().forEach(c -> newQuery.filter(expandMustNotClauses(c)));
      boolQuery.should().forEach(c -> newQuery.should(expandMustNotClauses(c)));

      for (QueryBuilder c : boolQuery.mustNot()) {
        if (c instanceof NestedQueryBuilder) {
          NestedQueryBuilder nestedClause = (NestedQueryBuilder) c;
          newQuery.should(expandMustNotClauses(
                  new NestedQueryBuilder(
                          nestedClause.innerHit().getName(),
                          new BoolQueryBuilder().mustNot(nestedClause.query()),
                          None
                  ).innerHit(nestedClause.innerHit())
          ));
        } else if (c instanceof BoolQueryBuilder) {
          BoolQueryBuilder boolClause = (BoolQueryBuilder) c;
          boolClause.filter().forEach(d -> newQuery.should(expandMustNotClauses(
                  new BoolQueryBuilder().mustNot(d)
          )));
          boolClause.should().forEach(d -> newQuery.filter(expandMustNotClauses(
                  new BoolQueryBuilder().mustNot(d)
          )));
          boolClause.mustNot().forEach(d -> newQuery.should(expandMustNotClauses(d)));
        } else {
          newQuery.mustNot(c);
        }
      }

      return newQuery;
    }

    /**
     * Implement ElasticsearchLogicalIndexAgg.
     */
    public PhysicalPlan visitIndexAggregation(ElasticsearchLogicalIndexAgg node,
                                              ElasticsearchIndexScan context) {
      QueryBuilder query = null;
      if (node.getFilter() != null) {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
            new DefaultExpressionSerializer());
        query = queryBuilder.build(node.getFilter());
        context.pushDown(query);
      }
      AggregationQueryBuilder builder =
          new AggregationQueryBuilder(new DefaultExpressionSerializer());
      List<AggregationBuilder> aggregationBuilder =
          builder.buildAggregationBuilder(node.getAggregatorList(),
              node.getGroupByList(), node.getSortList());
      if (query != null) {
        if (query instanceof NestedQueryBuilder) {
          query = ((NestedQueryBuilder) query).query();
        }
        List<AggregationBuilder> newAggregationBuilder = new ArrayList<>();
        for (AggregationBuilder ab : aggregationBuilder) {
          if (ab instanceof NestedAggregationBuilder) {
            NestedAggregationBuilder nestedAggregationBuilder
                    = new NestedAggregationBuilder(
                            ab.getName(), ((NestedAggregationBuilder) ab).path()
                    );
            for (AggregationBuilder sa : ab.getSubAggregations()) {
              nestedAggregationBuilder.subAggregation(
                      new FilterAggregationBuilder(sa.getName(), query).subAggregation(sa));
            }
            newAggregationBuilder.add(nestedAggregationBuilder);
          } else if (!(ab instanceof CompositeAggregationBuilder)) {
            newAggregationBuilder.add(new FilterAggregationBuilder(ab.getName(), query)
                    .subAggregation(ab));
          } else {
            newAggregationBuilder.add(ab);
          }
        }
        aggregationBuilder = newAggregationBuilder;
      }
      context.pushDownAggregation(aggregationBuilder);
      context.pushTypeMapping(
          builder.buildTypeMapping(node.getAggregatorList(),
              node.getGroupByList()));
      return indexScan;
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, ElasticsearchIndexScan context) {
      return indexScan;
    }
  }
}
