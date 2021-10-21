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

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.utils.Utils.indexScan;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.utils.Utils.indexScanAgg;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.utils.Utils.noProjects;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.utils.Utils.projects;
import static com.amazon.opendistroforelasticsearch.sql.expression.DSL.literal;
import static com.amazon.opendistroforelasticsearch.sql.expression.DSL.named;
import static com.amazon.opendistroforelasticsearch.sql.expression.DSL.ref;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.eval;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.filter;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.limit;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.project;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.relation;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.remove;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.rename;
import static com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL.sort;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.amazon.opendistroforelasticsearch.sql.ast.tree.Sort;
import com.amazon.opendistroforelasticsearch.sql.common.setting.Settings;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprBooleanValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.ElasticsearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value.ElasticsearchExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.mapping.IndexMapping;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.planner.logical.ElasticsearchLogicalIndexAgg;
import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.amazon.opendistroforelasticsearch.sql.expression.DSL;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.NamedExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.aggregation.AvgAggregator;
import com.amazon.opendistroforelasticsearch.sql.expression.aggregation.NamedAggregator;
import com.amazon.opendistroforelasticsearch.sql.expression.config.ExpressionConfig;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlanDSL;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.AggregationOperator;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.FilterOperator;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.LimitOperator;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlanDSL;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.ProjectOperator;
import com.amazon.opendistroforelasticsearch.sql.storage.Table;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ElasticsearchIndexTest {

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  @Mock
  private ElasticsearchClient client;

  @Mock
  private ElasticsearchExprValueFactory exprValueFactory;

  @Mock
  private Settings settings;

  @Test
  void getFieldTypes() {
    when(client.getIndexMappings("test"))
            .thenReturn(
                    ImmutableMap.of(
                            "test",
                            new IndexMapping(
                                    ImmutableMap.<String, String>builder()
                                            .put("name", "keyword")
                                            .put("address", "text")
                                            .put("age", "integer")
                                            .put("account_number", "long")
                                            .put("balance1", "float")
                                            .put("balance2", "double")
                                            .put("gender", "boolean")
                                            .put("family", "nested")
                                            .put("employer", "object")
                                            .put("birthday", "date")
                                            .put("id1", "byte")
                                            .put("id2", "short")
                                            .put("blob", "binary")
                                            .build())));

    Table index = new ElasticsearchIndex(client, settings, "test");
    Map<String, ExprType> fieldTypes = index.getFieldTypes();
    assertThat(
            fieldTypes,
            allOf(
                    aMapWithSize(13),
                    hasEntry("name", ExprCoreType.STRING),
                    hasEntry("address", (ExprType) ElasticsearchDataType.ES_TEXT),
                    hasEntry("age", ExprCoreType.INTEGER),
                    hasEntry("account_number", ExprCoreType.LONG),
                    hasEntry("balance1", ExprCoreType.FLOAT),
                    hasEntry("balance2", ExprCoreType.DOUBLE),
                    hasEntry("gender", ExprCoreType.BOOLEAN),
                    hasEntry("family", ExprCoreType.ARRAY),
                    hasEntry("employer", ExprCoreType.STRUCT),
                    hasEntry("birthday", ExprCoreType.TIMESTAMP),
                    hasEntry("id1", ExprCoreType.BYTE),
                    hasEntry("id2", ExprCoreType.SHORT),
                    hasEntry("blob", (ExprType) ElasticsearchDataType.ES_BINARY)
            ));
  }

  @Test
  void implementRelationOperatorOnly() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    LogicalPlan plan = relation(indexName);
    Table index = new ElasticsearchIndex(client, settings, indexName);
    assertEquals(
            new ElasticsearchIndexScan(client, settings, indexName, exprValueFactory),
            index.implement(plan));
  }

  @Test
  void implementRelationOperatorWithOptimization() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    LogicalPlan plan = relation(indexName);
    Table index = new ElasticsearchIndex(client, settings, indexName);
    assertEquals(
            new ElasticsearchIndexScan(client, settings, indexName, exprValueFactory),
            index.implement(index.optimize(plan)));
  }

  @Test
  void implementOtherLogicalOperators() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
            Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
                    DOUBLE)));
    Map<ReferenceExpression, ReferenceExpression> mappings =
            ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
            ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Integer sortCount = 100;
    Pair<Sort.SortOption, Expression> sortField =
            ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("name1", STRING));

    LogicalPlan plan =
            project(
                    LogicalPlanDSL.dedupe(
                            sort(
                                    eval(
                                            remove(
                                                    rename(
                                                            relation(indexName),
                                                            mappings),
                                                    exclude),
                                            newEvalField),
                                    sortField),
                            dedupeField),
                    include);

    Table index = new ElasticsearchIndex(client, settings, indexName);
    assertEquals(
            PhysicalPlanDSL.project(
                    PhysicalPlanDSL.dedupe(
                            PhysicalPlanDSL.sort(
                                    PhysicalPlanDSL.eval(
                                            PhysicalPlanDSL.remove(
                                                    PhysicalPlanDSL.rename(
                                                            new ElasticsearchIndexScan(
                                                                    client, settings, indexName,
                                                                    exprValueFactory),
                                                            mappings),
                                                    exclude),
                                            newEvalField),
                                    sortField),
                            dedupeField),
                    include),
            index.implement(plan));
  }

  @Test
  void shouldImplLogicalIndexScan() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);
    Expression filterExpr = dsl.equal(field, literal("John"));

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);
    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName,
                            filterExpr
                    ),
                    named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);
  }

  @Test
  void shouldNotPushDownFilterFarFromRelation() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    ReferenceExpression field = ref("name", STRING);
    Expression filterExpr = dsl.equal(field, literal("John"));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
            Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
                    DOUBLE)));

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);
    PhysicalPlan plan = index.implement(
            filter(
                    aggregation(
                            relation(indexName),
                            aggregators,
                            groupByExprs
                    ),
                    filterExpr));

    assertTrue(plan instanceof FilterOperator);
  }

  @Test
  void shouldImplLogicalIndexScanAgg() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    ReferenceExpression field = ref("name", STRING);
    Expression filterExpr = dsl.equal(field, literal("John"));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
            Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
                    DOUBLE)));

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    // IndexScanAgg without Filter
    PhysicalPlan plan = index.implement(
            filter(
                    indexScanAgg(
                            indexName,
                            aggregators,
                            groupByExprs
                    ),
                    filterExpr));

    assertTrue(plan.getChild().get(0) instanceof ElasticsearchIndexScan);

    // IndexScanAgg with Filter
    plan = index.implement(
            indexScanAgg(
                    indexName,
                    filterExpr,
                    aggregators,
                    groupByExprs));
    assertTrue(plan instanceof ElasticsearchIndexScan);
  }

  @Test
  void shouldNotPushDownAggregationFarFromRelation() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    ReferenceExpression field = ref("name", STRING);
    Expression filterExpr = dsl.equal(field, literal("John"));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
            Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
                    DOUBLE)));

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    PhysicalPlan plan = index.implement(
            aggregation(
                    filter(filter(
                            relation(indexName),
                            filterExpr), filterExpr),
                    aggregators,
                    groupByExprs));
    assertTrue(plan instanceof AggregationOperator);
  }

  @Test
  void shouldImplIndexScanWithSort() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);
    Expression sortExpr = ref("name", STRING);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);
    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName,
                            Pair.of(Sort.SortOption.DEFAULT_ASC, sortExpr)
                    ),
                    named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);
  }

  @Test
  void shouldImplIndexScanWithLimit() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);
    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName,
                            1, 1, noProjects()
                    ),
                    named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);
  }

  @Test
  void shouldImplIndexScanWithSortAndLimit() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);
    Expression sortExpr = ref("name", STRING);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);
    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName,
                            null,
                            1, 1,
                            Stream.of(Pair.of(Sort.SortOption.DEFAULT_ASC, sortExpr))
                                    .collect(Collectors.toList()),
                            noProjects()
                    ),
                    named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);
  }

  @Test
  void shouldNotPushDownLimitFarFromRelationButUpdateScanSize() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);
    PhysicalPlan plan = index.implement(index.optimize(
            project(
                    limit(
                            sort(
                                    relation("test"),
                                    Pair.of(Sort.SortOption.DEFAULT_ASC,
                                            dsl.abs(named("intV", ref("intV", INTEGER))))
                            ),
                            300, 1
                    ),
                    named("intV", ref("intV", INTEGER))
            )
    ));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof LimitOperator);
  }

  @Test
  void shouldPushDownProjects() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);
    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName, projects(ref("intV", INTEGER))
                    ),
                    named("i", ref("intV", INTEGER))));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);

    final FetchSourceContext fetchSource =
            ((ElasticsearchIndexScan) ((ProjectOperator) plan).getInput()).getRequest()
                    .getSourceBuilder().fetchSource();
    assertThat(fetchSource.includes(), arrayContaining("intV"));
    assertThat(fetchSource.excludes(), emptyArray());
  }

  @Test
  void shouldPushDownProjectsAndFilter() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    ReferenceExpression expr = ref("field", INTEGER);
    Expression filterExpr = dsl.equal(expr, DSL.literal(1));

    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName, filterExpr, projects(expr)
                    ),
                    named("field", expr)
            )
    );

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);

    final FetchSourceContext fetchSource =
            ((ElasticsearchIndexScan) ((ProjectOperator) plan).getInput()).getRequest()
                    .getSourceBuilder().fetchSource();
    assertThat(fetchSource.includes(), arrayContaining("field"));
    assertThat(fetchSource.excludes(), emptyArray());

    final QueryBuilder query =
            ((ElasticsearchIndexScan) ((ProjectOperator) plan).getInput()).getRequest()
                    .getSourceBuilder().query();
    assertTrue(query instanceof BoolQueryBuilder);
    assertTrue(((BoolQueryBuilder) query).must().get(0) instanceof TermQueryBuilder);
  }

  @Test
  void shouldVisitIndexScanWithNestedFilter() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    ReferenceExpression firstNestedExpr = DSL.nested("firstArray.field", "firstArray", INTEGER);
    ReferenceExpression secondNestedExpr = DSL.nested("secondArray.field", "secondArray", INTEGER);
    Expression filterExpr = dsl.equal(firstNestedExpr, DSL.literal(1));

    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName, filterExpr, projects(firstNestedExpr, secondNestedExpr)
                    ),
                    named("firstNestedField", firstNestedExpr)
            )
    );

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);

    final QueryBuilder queryBuilder = ((ElasticsearchIndexScan) ((ProjectOperator) plan)
            .getInput()).getRequest().getSourceBuilder().query();
    assertTrue(queryBuilder instanceof BoolQueryBuilder);
    assertTrue(((BoolQueryBuilder) queryBuilder).must().get(0) instanceof NestedQueryBuilder);
  }

  @Test
  void shouldVisitIndexScanWithNestedProjects() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    ReferenceExpression nestedExpr = DSL.nested("array.field", "array", INTEGER);
    Expression filterExpr = dsl.equal(ref("anotherField", DOUBLE), DSL.literal(1f));

    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName, filterExpr, projects(nestedExpr)
                    ),
                    named("nestedField", nestedExpr)
            )
    );

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);

    final QueryBuilder queryBuilder = ((ElasticsearchIndexScan) ((ProjectOperator) plan)
            .getInput()).getRequest().getSourceBuilder().query();
    assertTrue(queryBuilder instanceof BoolQueryBuilder);
    assertTrue(((BoolQueryBuilder) queryBuilder).must().get(0) instanceof TermQueryBuilder);
    assertTrue(((BoolQueryBuilder) queryBuilder).must().get(1) instanceof NestedQueryBuilder);
  }

  @Test
  void shouldVisitIndexAggWithFilter() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    ReferenceExpression field = ref("field", INTEGER);
    Expression filterExpr = dsl.equal(field, literal(3));
    List<NamedAggregator> aggregators =
            Arrays.asList(named("avg(field)",
                    new AvgAggregator(Arrays.asList(field), DOUBLE)));

    PhysicalPlan plan = index.implement(
            new ElasticsearchLogicalIndexAgg(
                    "name",
                    filterExpr,
                    aggregators,
                    new ArrayList<>(),
                    new ArrayList<>()
            )
    );

    assertTrue(plan instanceof ElasticsearchIndexScan);
    final SearchSourceBuilder sourceBuilder = ((ElasticsearchIndexScan) plan)
            .getRequest().getSourceBuilder();
    assertTrue(sourceBuilder.query() instanceof TermQueryBuilder);
    assertEquals(1, sourceBuilder.aggregations().getAggregatorFactories().size());
    assertTrue(sourceBuilder.aggregations()
            .getAggregatorFactories().toArray()[0] instanceof FilterAggregationBuilder);
  }

  @Test
  void shouldVisitIndexAggWithNestedFilterAndFields() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    ReferenceExpression nestedExpr = DSL.nested("array.field", "array", INTEGER);
    Expression filterExpr = dsl.equal(nestedExpr, literal(3));
    List<NamedAggregator> aggregators =
            Arrays.asList(named("avg(array.field)",
                    new AvgAggregator(Arrays.asList(nestedExpr), DOUBLE)));

    PhysicalPlan plan = index.implement(
            new ElasticsearchLogicalIndexAgg(
                "name",
                filterExpr,
                aggregators,
                new ArrayList<>(),
                new ArrayList<>()
            )
    );

    assertTrue(plan instanceof ElasticsearchIndexScan);
    final SearchSourceBuilder sourceBuilder = ((ElasticsearchIndexScan) plan)
            .getRequest().getSourceBuilder();
    assertTrue(sourceBuilder.query() instanceof NestedQueryBuilder);
    assertEquals(1, sourceBuilder.aggregations().getAggregatorFactories().size());
    assertTrue(sourceBuilder.aggregations()
            .getAggregatorFactories().toArray()[0] instanceof NestedAggregationBuilder);
  }

  @Test
  void incompatibleNestedClausesShouldThrowException() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    Expression filterExpr = dsl.and(
            dsl.or(
                    dsl.less(
                            DSL.nested("path.firstField", "path", INTEGER),
                            literal(2)),
                    dsl.equal(
                            DSL.ref("normalField", INTEGER),
                            literal(10)
                    )
            ),
            dsl.greater(
                    DSL.nested("path.secondField", "path", STRING),
                    literal("John")
            )
    );

    ExpressionEvaluationException e = assertThrows(
            ExpressionEvaluationException.class, () -> index.implement(
                    project(
                            indexScan(
                                    indexName, filterExpr
                            )
                    )
            )
    );
    assertEquals(
            "Cannot have more than one occurrence of the same nested path in WHERE statement",
            e.getMessage()
    );
  }

  @Test
  void shouldVisitComplexIndexScan() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    Expression filterExpr = dsl.and(
            dsl.or(
                    dsl.less(
                            DSL.nested("firstPath.firstField", "firstPath", INTEGER),
                            literal(2)),
                    dsl.equal(
                            DSL.ref("normalField", INTEGER),
                            literal(10)
                    )
            ),
            dsl.not(
                    dsl.greater(
                            DSL.nested("secondPath.secondField", "secondPath", STRING),
                            literal("John")
                    )
            )
    );

    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName, filterExpr
                    )
            )
    );

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);

    final QueryBuilder queryBuilder = ((ElasticsearchIndexScan) ((ProjectOperator) plan)
            .getInput()).getRequest().getSourceBuilder().query();
    assertTrue(queryBuilder instanceof BoolQueryBuilder);
  }

  @Test
  void shouldExpandMustNotClauseInIndexScan() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(settings.getSettingValue(Settings.Key.SQL_NESTED_FLATTENED_LIMIT)).thenReturn(0);

    String indexName = "test";
    ElasticsearchIndex index = new ElasticsearchIndex(client, settings, indexName);

    Expression filterExpr = dsl.not(
            dsl.and(
                    dsl.or(
                            dsl.less(
                                    DSL.nested("firstPath.firstField", "firstPath", INTEGER),
                                    literal(2)
                            ),
                            dsl.equal(
                                    DSL.ref("normalField", INTEGER),
                                    literal(10)
                            )
                    ),
                    dsl.not(
                            dsl.greater(
                                    DSL.nested("secondPath.secondField", "secondPath", STRING),
                                    literal("John")
                            )
                    )
            )
    );

    PhysicalPlan plan = index.implement(
            project(
                    indexScan(
                            indexName, filterExpr
                    )
            )
    );

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof ElasticsearchIndexScan);

    final QueryBuilder queryBuilder = ((ElasticsearchIndexScan) ((ProjectOperator) plan)
            .getInput()).getRequest().getSourceBuilder().query();
    assertTrue(queryBuilder instanceof BoolQueryBuilder);
    QueryBuilder innerQueryBuilder = ((BoolQueryBuilder) queryBuilder).must().get(0);
    assertTrue(innerQueryBuilder instanceof BoolQueryBuilder);
    assertTrue(((BoolQueryBuilder) innerQueryBuilder).mustNot().isEmpty());
  }
}