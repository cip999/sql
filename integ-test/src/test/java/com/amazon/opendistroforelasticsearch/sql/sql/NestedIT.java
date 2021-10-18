/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.sql;

import static com.amazon.opendistroforelasticsearch.sql.legacy.TestsConstants.TEST_INDEX_EMPLOYEE_NESTED;
import static com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.rows;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.schema;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.verifyDataRows;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.verifySchema;
import static com.amazon.opendistroforelasticsearch.sql.util.TestUtils.getResponseBody;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazon.opendistroforelasticsearch.sql.legacy.SQLIntegTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.json.JSONObject;
import org.junit.Test;


import java.io.IOException;
import java.util.Locale;


public class NestedIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.EMPLOYEE_NESTED);
  }

  @Test
  public void simpleNestedFieldInSelect() throws IOException {
    JSONObject result =
            executeQuery(String.format(
                    "SELECT comments.likes FROM %s", TEST_INDEX_EMPLOYEE_NESTED
            ));
    verifySchema(result,
            schema("comments.likes", null, "long"));
    verifyDataRows(result,
            rows(56),
            rows(22),
            rows(56),
            rows(22),
            rows(24),
            rows(42),
            rows(82),
            rows(14));
  }

  @Test
  public void multipleNestedFieldsWithLimitAndSort() throws IOException {
    JSONObject result =
            executeQuery(String.format(
                    "SELECT id, projects.started_year, comments.date, comments.likes FROM %s "
                            + "ORDER BY id DESC LIMIT 5",
                    TEST_INDEX_EMPLOYEE_NESTED
            ));
    verifySchema(result,
            schema("id", null, "long"),
            schema("projects.started_year", null, "long"),
            schema("comments.date", null, "timestamp"),
            schema("comments.likes", null, "long"));
    verifyDataRows(result,
            rows(6, 1998, "2018-06-23 00:00:00", 24),
            rows(6, 1998, "2017-10-25 00:00:00", 42),
            rows(6, 2015, "2018-06-23 00:00:00", 24),
            rows(6, 2015, "2017-10-25 00:00:00", 42),
            rows(3, 1990, "2018-06-23 00:00:00", 56));
  }

  @Test
  public void simpleWhereStatementWithNestedField() throws IOException {
    JSONObject result =
            executeQuery(String.format(
                    "SELECT comments.likes FROM %s "
                            + "WHERE comments.likes > 30",
                    TEST_INDEX_EMPLOYEE_NESTED
            ));
    verifySchema(result,
            schema("comments.likes", null, "long"));
    verifyDataRows(result,
            rows(56),
            rows(56),
            rows(42),
            rows(82));
  }

  @Test
  public void complexWhereStatementWithMultipleNestedFields() throws IOException {
    JSONObject result =
            executeQuery(String.format(
                    "SELECT projects.started_year, comments.date FROM %s "
                            + "WHERE not projects.started_year <> 1999 and comments.likes > 30",
                    TEST_INDEX_EMPLOYEE_NESTED
            ));
    verifySchema(result,
            schema("projects.started_year", null, "long"),
            schema("comments.date", null, "timestamp"));
    verifyDataRows(result,
            rows(1999, "2018-06-23 00:00:00"));
  }

  @Test
  public void mustNotClauseInOuterLevelOfWhereStatement() throws IOException {
    JSONObject result =
            executeQuery(String.format(
                    "SELECT comments.message FROM %s "
                            + "WHERE not (id < 5 or not comments.likes = 42)",
                    TEST_INDEX_EMPLOYEE_NESTED
            ));
    verifySchema(result,
            schema("comments.message", null, "text"));
    verifyDataRows(result,
            rows("comment_3_2"));
  }

  @Test
  public void incompatibleNestedFieldsInWhereStatement() {
    assertThrows(
            Exception.class,
            () -> executeQuery(String.format(
                    "SELECT comments.likes FROM %s "
                            + "WHERE comments.likes < 30 or (id = 6 and comments.likes = 42)",
                    TEST_INDEX_EMPLOYEE_NESTED
            ))
    );
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }

}
