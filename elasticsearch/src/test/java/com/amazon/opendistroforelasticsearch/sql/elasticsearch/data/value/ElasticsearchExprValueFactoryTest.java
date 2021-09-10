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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value;

import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.booleanValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.byteValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.doubleValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.floatValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.integerValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.longValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.nullValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.shortValue;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.stringValue;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.ARRAY;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.BOOLEAN;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.BYTE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DATE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DATETIME;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.FLOAT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.LONG;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.SHORT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRUCT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.TIME;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType.ES_BINARY;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType.ES_GEO_POINT;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType.ES_IP;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType.ES_TEXT;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType.ES_TEXT_KEYWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprCollectionValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDateValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDatetimeValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprNullValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTimeValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTimestampValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTupleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.utils.ElasticsearchJsonContent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.Test;

class ElasticsearchExprValueFactoryTest {

  private static final Map<String, ExprType> MAPPING =
      new ImmutableMap.Builder<String, ExprType>()
          .put("byteV", BYTE)
          .put("shortV", SHORT)
          .put("intV", INTEGER)
          .put("longV", LONG)
          .put("floatV", FLOAT)
          .put("doubleV", DOUBLE)
          .put("stringV", STRING)
          .put("dateV", DATE)
          .put("datetimeV", DATETIME)
          .put("timeV", TIME)
          .put("timestampV", TIMESTAMP)
          .put("boolV", BOOLEAN)
          .put("structV", STRUCT)
          .put("structV.id", INTEGER)
          .put("structV.state", STRING)
          .put("arrayV", ARRAY)
          .put("arrayV.info", STRING)
          .put("arrayV.author", STRING)
          .put("book", STRUCT)
          .put("book.price", DOUBLE)
          .put("book.author", ARRAY)
          .put("book.author.name", STRING)
          .put("book.author.age", INTEGER)
          .put("textV", ES_TEXT)
          .put("textKeywordV", ES_TEXT_KEYWORD)
          .put("ipV", ES_IP)
          .put("geoV", ES_GEO_POINT)
          .put("binaryV", ES_BINARY)
          .build();
  private ElasticsearchExprValueFactory exprValueFactory =
      new ElasticsearchExprValueFactory(MAPPING);

  @Test
  public void constructNullValue() {
    assertEquals(nullValue(), tupleValue("{\"intV\":null}").get("intV"));
    assertEquals(nullValue(), constructFromObject("intV",  null));
    assertTrue(new ElasticsearchJsonContent(null).isNull());
  }

  @Test
  public void constructByte() {
    assertEquals(byteValue((byte) 1), tupleValue("{\"byteV\":1}").get("byteV"));
    assertEquals(byteValue((byte) 1), constructFromObject("byteV", 1));
  }

  @Test
  public void constructShort() {
    assertEquals(shortValue((short) 1), tupleValue("{\"shortV\":1}").get("shortV"));
    assertEquals(shortValue((short) 1), constructFromObject("shortV", 1));
  }

  @Test
  public void constructInteger() {
    assertEquals(integerValue(1), tupleValue("{\"intV\":1}").get("intV"));
    assertEquals(integerValue(1), constructFromObject("intV", 1));
  }

  @Test
  public void constructIntegerValueInStringValue() {
    assertEquals(integerValue(1), constructFromObject("intV", "1"));
  }

  @Test
  public void constructLong() {
    assertEquals(longValue(1L), tupleValue("{\"longV\":1}").get("longV"));
    assertEquals(longValue(1L), constructFromObject("longV", 1L));
  }

  @Test
  public void constructFloat() {
    assertEquals(floatValue(1f), tupleValue("{\"floatV\":1.0}").get("floatV"));
    assertEquals(floatValue(1f), constructFromObject("floatV", 1f));
  }

  @Test
  public void constructDouble() {
    assertEquals(doubleValue(1d), tupleValue("{\"doubleV\":1.0}").get("doubleV"));
    assertEquals(doubleValue(1d), constructFromObject("doubleV", 1d));
  }

  @Test
  public void constructString() {
    assertEquals(stringValue("text"), tupleValue("{\"stringV\":\"text\"}").get("stringV"));
    assertEquals(stringValue("text"), constructFromObject("stringV", "text"));
  }

  @Test
  public void constructBoolean() {
    assertEquals(booleanValue(true), tupleValue("{\"boolV\":true}").get("boolV"));
    assertEquals(booleanValue(true), constructFromObject("boolV", true));
  }

  @Test
  public void constructText() {
    assertEquals(new ElasticsearchExprTextValue("text"),
                 tupleValue("{\"textV\":\"text\"}").get("textV"));
    assertEquals(new ElasticsearchExprTextValue("text"),
                 constructFromObject("textV", "text"));

    assertEquals(new ElasticsearchExprTextKeywordValue("text"),
                 tupleValue("{\"textKeywordV\":\"text\"}").get("textKeywordV"));
    assertEquals(new ElasticsearchExprTextKeywordValue("text"),
                 constructFromObject("textKeywordV", "text"));
  }

  @Test
  public void constructDate() {
    assertEquals(
        new ExprTimestampValue("2015-01-01 00:00:00"),
        tupleValue("{\"timestampV\":\"2015-01-01\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        tupleValue("{\"timestampV\":\"2015-01-01T12:10:30Z\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        tupleValue("{\"timestampV\":\"2015-01-01T12:10:30\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        tupleValue("{\"timestampV\":\"2015-01-01 12:10:30\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2020-08-17T19:44:00.100500"),
        tupleValue("{\"timestampV\":\"2020-08-17T19:44:00.100500\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2020-08-17 19:44:00.100500"),
        tupleValue("{\"timestampV\":\"2020-08-17 19:44:00.100500\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        tupleValue("{\"timestampV\":1420070400001}").get("timestampV"));
    assertEquals(
        new ExprTimeValue("19:36:22"),
        tupleValue("{\"timestampV\":\"19:36:22\"}").get("timestampV"));

    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        constructFromObject("timestampV", 1420070400001L));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        constructFromObject("timestampV", Instant.ofEpochMilli(1420070400001L)));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        constructFromObject("timestampV", "2015-01-01 12:10:30"));
    assertEquals(
        new ExprDateValue("2015-01-01"),
        constructFromObject("dateV","2015-01-01"));
    assertEquals(
        new ExprTimeValue("12:10:30"),
        constructFromObject("timeV","12:10:30"));
    assertEquals(
        new ExprDatetimeValue("2015-01-01 12:10:30"),
        constructFromObject("datetimeV", "2015-01-01 12:10:30"));
  }

  @Test
  public void constructDateFromUnsupportedFormatThrowException() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () ->
                tupleValue("{\"timestampV\":\"2015-01-01 1:10:10\"}"));
    assertEquals(
        "Construct ExprTimestampValue from \"2015-01-01 1:10:10\" failed, "
            + "unsupported date format.",
        exception.getMessage());
  }

  @Test
  public void constructArray() {
    assertEquals(
            new ExprCollectionValue(ImmutableList.of(new ExprTupleValue(
                  new LinkedHashMap<String, ExprValue>() {
                    {
                      put("info", stringValue("zz"));
                      put("author", stringValue("au"));
                    }
                  })
            )),
        tupleValue("{\"arrayV\":[{\"info\":\"zz\",\"author\":\"au\"}]}").get("arrayV"));
    assertEquals(
            new ExprCollectionValue(ImmutableList.of(new ExprTupleValue(
                  new LinkedHashMap<String, ExprValue>() {
                    {
                      put("info", stringValue("zz"));
                      put("author", stringValue("au"));
                    }
                  })
            )),
        constructFromObject("arrayV", ImmutableList.of(
            ImmutableMap.of("info", "zz", "author", "au"))));
  }

  @Test
  public void constructStruct() {
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<String, ExprValue>() {
              {
                put("id", integerValue(1));
                put("state", stringValue("WA"));
              }
            }),
        tupleValue("{\"structV\":{\"id\":1,\"state\":\"WA\"}}").get("structV"));
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<String, ExprValue>() {
              {
                put("id", integerValue(1));
                put("state", stringValue("WA"));
              }
            }),
        constructFromObject("structV", ImmutableMap.of("id", 1, "state", "WA")));
  }

  @Test
  public void constructIP() {
    assertEquals(new ElasticsearchExprIpValue("192.168.0.1"),
        tupleValue("{\"ipV\":\"192.168.0.1\"}").get("ipV"));
  }

  @Test
  public void constructGeoPoint() {
    assertEquals(new ElasticsearchExprGeoPointValue(42.60355556, -97.25263889),
        tupleValue("{\"geoV\":{\"lat\":42.60355556,\"lon\":-97.25263889}}").get("geoV"));
    assertEquals(new ElasticsearchExprGeoPointValue(42.60355556, -97.25263889),
        tupleValue("{\"geoV\":{\"lat\":\"42.60355556\",\"lon\":\"-97.25263889\"}}").get("geoV"));
    assertEquals(new ElasticsearchExprGeoPointValue(42.60355556, -97.25263889),
        constructFromObject("geoV", "42.60355556,-97.25263889"));
  }

  @Test
  public void constructGeoPointFromUnsupportedFormatShouldThrowException() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":[42.60355556,-97.25263889]}").get("geoV"));
    assertEquals("geo point must in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lon\":-97.25263889}}").get("geoV"));
    assertEquals("geo point must in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":-97.25263889}}").get("geoV"));
    assertEquals("geo point must in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":true,\"lon\":-97.25263889}}").get("geoV"));
    assertEquals("latitude must be number value, but got value: true", exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":42.60355556,\"lon\":false}}").get("geoV"));
    assertEquals("longitude must be number value, but got value: false", exception.getMessage());
  }

  @Test
  public void constructBinary() {
    assertEquals(new ElasticsearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg=="),
        tupleValue("{\"binaryV\":\"U29tZSBiaW5hcnkgYmxvYg==\"}").get("binaryV"));
  }

  /**
   * Return the first element if is Elasticsearch Array.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html.
   */
  @Test
  public void constructFromElasticsearchArrayReturnFirstElement() {
    assertEquals(integerValue(1), tupleValue("{\"intV\":[1, 2, 3]}").get("intV"));
    assertEquals(new ExprTupleValue(
        new LinkedHashMap<String, ExprValue>() {
          {
            put("id", integerValue(1));
            put("state", stringValue("WA"));
          }
        }), tupleValue("{\"structV\":[{\"id\":1,\"state\":\"WA\"},{\"id\":2,\"state\":\"CA\"}]}}")
        .get("structV"));
  }

  @Test
  public void constructHitWithInnerHits() {
    SearchHit firstInnerHit = new SearchHit(1)
            .sourceRef(new BytesArray("{\"name\": \"John\", \"age\": 22}"));
    SearchHit secondInnerHit = new SearchHit(1)
            .sourceRef(new BytesArray("{\"name\": \"Mark\", \"age\": 67}"));
    SearchHit hit = new SearchHit(1)
            .sourceRef(new BytesArray("{\"doubleV\": 2.3, \"book\": {"
                    + "\"price\": 20.99, \"author\": ["
                    + "{\"name\": \"John\", \"age\": 22}, "
                    + "{\"name\": \"Mark\", \"age\": 67}]}}"));
    hit.setInnerHits(Map.of("book.author",
            new SearchHits(new SearchHit[]{firstInnerHit, secondInnerHit},
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1f)));

    List<ExprValue> construct = constructFromHit(hit, exprValueFactory);

    assertEquals(2, construct.size());
  }

  @Test
  public void constructHitWithInnerHitsAndLimit() {
    ElasticsearchExprValueFactory exprValueFactory = new ElasticsearchExprValueFactory(
            MAPPING, 2
    );

    SearchHit firstInnerHit = new SearchHit(1)
            .sourceRef(new BytesArray("{\"name\": \"John\", \"age\": 22}"));
    SearchHit secondInnerHit = new SearchHit(1)
            .sourceRef(new BytesArray("{\"name\": \"Mark\", \"age\": 67}"));
    SearchHit thirdInnerHit = new SearchHit(1)
            .sourceRef(new BytesArray("{\"name\": \"Eve\", \"age\": 12}"));
    SearchHit hit = new SearchHit(1)
            .sourceRef(new BytesArray("{\"doubleV\": 2.3, \"book\": {\"author\": ["
                    + "{\"name\": \"John\", \"age\": 22}, "
                    + "{\"name\": \"Mark\", \"age\": 67}, "
                    + "{\"name\": \"Eve\", \"age\": 12}]}}"));
    hit.setInnerHits(Map.of("book.author",
            new SearchHits(new SearchHit[]{firstInnerHit, secondInnerHit, thirdInnerHit},
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO), 1f)));

    List<ExprValue> construct = constructFromHit(hit, exprValueFactory);

    assertEquals(2, construct.size());
  }

  @Test
  public void constructEmptyOrNullArray() {
    assertEquals(ExprNullValue.of(), tupleValue("{\"arrayV\": []}").get("arrayV"));
    assertEquals(ExprNullValue.of(), tupleValue("{\"arrayV\": null}").get("arrayV"));
  }

  @Test
  public void constructNullAndEmptyStruct() {
    assertEquals(ExprNullValue.of(), tupleValue("{\"structV\": null}").get("structV"));
    assertEquals(ExprNullValue.of(), tupleValue("{\"structV\": {}}").get("structV"));
  }

  @Test
  public void constructFromInvalidJsonThrowException() {
    IllegalStateException stringException =
        assertThrows(IllegalStateException.class, () -> tupleValue("{\"invalid_json:1}"));
    assertEquals("Invalid json: {\"invalid_json:1}", stringException.getMessage());

    SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{\"invalid_json:1}"));
    IllegalStateException hitException =
            assertThrows(
                    IllegalStateException.class, () -> constructFromHit(hit, exprValueFactory)
            );
    assertEquals("Invalid json: {\"invalid_json:1}", hitException.getMessage());
  }

  @Test
  public void noTypeFoundForMappingThrowException() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> tupleValue("{\"not_exist\":1}"));
    assertEquals("No type found for field: not_exist", exception.getMessage());
  }

  @Test
  public void constructUnsupportedTypeThrowException() {
    ElasticsearchExprValueFactory exprValueFactory =
        new ElasticsearchExprValueFactory(ImmutableMap.of("type", new TestType()));
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> exprValueFactory.construct("{\"type\":1}"));
    assertEquals("Unsupported type: TEST_TYPE for value: 1", exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class, () -> exprValueFactory.construct("type", 1));
    assertEquals(
        "Unsupported type: TEST_TYPE for value: 1",
        exception.getMessage());
  }

  public Map<String, ExprValue> tupleValue(String jsonString) {
    final List<ExprValue> construct = exprValueFactory.construct(jsonString);
    return construct.isEmpty() ? null : construct.get(0).tupleValue();
  }

  private ExprValue constructFromObject(String fieldName, Object value) {
    final List<ExprValue> construct = exprValueFactory.construct(fieldName, value);
    return construct.isEmpty() ? null : construct.get(0);
  }

  private List<ExprValue> constructFromHit(SearchHit hit, ElasticsearchExprValueFactory factory) {
    return factory.construct(hit, "", STRUCT);
  }

  @EqualsAndHashCode
  @ToString
  private static class TestType implements ExprType {

    @Override
    public String typeName() {
      return "TEST_TYPE";
    }
  }
}
