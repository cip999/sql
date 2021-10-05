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

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprBooleanValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprByteValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprCollectionValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDateValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDatetimeValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDoubleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprFloatValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprIntegerValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprLongValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprNullValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprShortValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprStringValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTimeValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTimestampValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTupleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.data.utils.ExprDateFormatters;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.utils.Content;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.utils.ElasticsearchJsonContent;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.utils.ObjectContent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Setter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Construct ExprValue from Elasticsearch response.
 */
public class ElasticsearchExprValueFactory {
  /**
   * The Mapping of Field and ExprType.
   */
  @Setter
  private Map<String, ExprType> typeMapping;

  private Integer limit;

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<ExprType, Function<Content, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, Function<Content, ExprValue>>()
          .put(INTEGER, c -> new ExprIntegerValue(c.intValue()))
          .put(LONG, c -> new ExprLongValue(c.longValue()))
          .put(SHORT, c -> new ExprShortValue(c.shortValue()))
          .put(BYTE, c -> new ExprByteValue(c.byteValue()))
          .put(FLOAT, c -> new ExprFloatValue(c.floatValue()))
          .put(DOUBLE, c -> new ExprDoubleValue(c.doubleValue()))
          .put(STRING, c -> new ExprStringValue(c.stringValue()))
          .put(BOOLEAN, c -> ExprBooleanValue.of(c.booleanValue()))
          .put(TIMESTAMP, this::parseTimestamp)
          .put(DATE, c -> new ExprDateValue(parseTimestamp(c).dateValue().toString()))
          .put(TIME, c -> new ExprTimeValue(parseTimestamp(c).timeValue().toString()))
          .put(DATETIME, c -> new ExprDatetimeValue(parseTimestamp(c).datetimeValue()))
          .put(ES_TEXT, c -> new ElasticsearchExprTextValue(c.stringValue()))
          .put(ES_TEXT_KEYWORD, c -> new ElasticsearchExprTextKeywordValue(c.stringValue()))
          .put(ES_IP, c -> new ElasticsearchExprIpValue(c.stringValue()))
          .put(ES_GEO_POINT, c -> new ElasticsearchExprGeoPointValue(c.geoValue().getLeft(),
              c.geoValue().getRight()))
          .put(ES_BINARY, c -> new ElasticsearchExprBinaryValue(c.stringValue()))
          .build();

  public ElasticsearchExprValueFactory(Map<String, ExprType> typeMapping) {
    this(typeMapping, null);
  }

  public ElasticsearchExprValueFactory(Map<String, ExprType> typeMapping, Integer limit) {
    this.typeMapping = typeMapping;
    this.limit = limit;
  }

  /**
   * The struct construction has the following assumption. 1. The field has Elasticsearch Object
   * data type. https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html 2. The
   * deeper field is flattened in the typeMapping. e.g. {"employ", "STRUCT"} {"employ.id",
   * "INTEGER"} {"employ.state", "STRING"}
   */
  public List<ExprValue> construct(String jsonString) {
    try {
      return parse(new ElasticsearchJsonContent(OBJECT_MAPPER.readTree(jsonString)), TOP_PATH,
          STRUCT, null, new HashSet<>());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(String.format("Invalid json: %s", jsonString), e);
    }
  }

  /**
   * Construct ExprValue list from field and its value object. Throw exception if trying
   * to construct from field of unsupported type.
   * Todo, add IP, GeoPoint support after we have function implementation around it.
   *
   * @param field field name
   * @param value value object
   * @return List of ExprValue
   */
  public List<ExprValue> construct(String field, Object value) {
    return parse(new ObjectContent(value), field, type(field), null, new HashSet<>());
  }

  /**
   * Construct ExprValue list from SearchHit.
   *
   * @param hit search hit
   * @param field field name
   * @param type value object
   * @return List of ExprValue
   */
  public List<ExprValue> construct(SearchHit hit, String field, ExprType type) {
    String sourceAsString = hit.getSourceAsString();
    Content content;
    try {
      content = new ElasticsearchJsonContent(OBJECT_MAPPER.readTree(sourceAsString));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(String.format("Invalid json: %s", sourceAsString), e);
    }

    Set<String> alreadyProcessed = new HashSet<>();
    return parse(content, field, type, hit.getInnerHits(), alreadyProcessed);
  }

  private List<ExprValue> parse(Content content, String field, ExprType type,
                                Map<String, SearchHits> innerHits, Set<String> alreadyProcessed) {
    if (alreadyProcessed.contains(field)) {
      return new ArrayList<>();
    }

    if (type == STRUCT) {
      if (content.isNull() || !content.map().hasNext()) {
        return Collections.singletonList(ExprNullValue.of());
      } else {
        return parseStruct(content, field, innerHits, alreadyProcessed);
      }
    } else {
      if (content.isNull()) {
        return Collections.singletonList(ExprNullValue.of());
      }
      if (type == ARRAY) {
        return Collections.singletonList(parseArray(content, field));
      } else {
        if (typeActionMap.containsKey(type)) {
          return Stream.of(typeActionMap.get(type).apply(content)).collect(Collectors.toList());
        } else {
          throw new IllegalStateException(
                  String.format(
                          "Unsupported type: %s for value: %s",
                          type.typeName(), content.objectValue()
                  ));
        }
      }
    }
  }

  private ExprType type(String field) {
    if (typeMapping.containsKey(field)) {
      return typeMapping.get(field);
    } else {
      throw new IllegalStateException(String.format("No type found for field: %s", field));
    }
  }

  /**
   * Only default strict_date_optional_time||epoch_millis is supported.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
   * The customized date_format is not supported.
   */
  private ExprValue constructTimestamp(String value) {
    try {
      return new ExprTimestampValue(
          // Using Elasticsearch DateFormatters for now.
          DateFormatters.from(ExprDateFormatters.TOLERANT_PARSER_DATE_TIME_FORMATTER
              .parse(value)).toInstant());
    } catch (DateTimeParseException e) {
      throw new IllegalStateException(
          String.format(
              "Construct ExprTimestampValue from \"%s\" failed, unsupported date format.", value),
          e);
    }
  }

  private ExprValue parseTimestamp(Content value) {
    if (value.isNumber()) {
      return new ExprTimestampValue(Instant.ofEpochMilli(value.longValue()));
    } else if (value.isString()) {
      return constructTimestamp(value.stringValue());
    } else {
      return new ExprTimestampValue((Instant) value.objectValue());
    }
  }

  private List<ExprValue> parseStruct(Content content, String field,
                                      Map<String, SearchHits> innerHits,
                                      Set<String> alreadyProcessed) {
    if (innerHits == null) {
      innerHits = new HashMap<>();
    }

    List<ExprValue> result = Stream.of(new ExprTupleValue(new LinkedHashMap<>()))
            .collect(Collectors.toList());
    Map<String, List<ExprValue>> innerResults = new HashMap<>();
    Integer initialLimit = limit;
    Integer currentLimit = limit;

    for (String key : innerHits.keySet()) {
      List<ExprValue> innerResult = new ArrayList<>();
      alreadyProcessed.add(key);
      for (SearchHit hit : innerHits.get(key).getHits()) {
        innerResult.addAll(construct(hit, makeField(field, key), STRUCT));
      }
      innerResults.put(key, innerResult);
      currentLimit = ratioCeil(currentLimit, innerResult.size());
      limit = currentLimit;
    }

    limit = null;
    Iterator<Map.Entry<String, Content>> it = content.map();
    while (it.hasNext()) {
      Map.Entry<String, Content> entry = it.next();
      List<ExprValue> innerResult = parse(entry.getValue(), makeField(field, entry.getKey()),
              type(makeField(field, entry.getKey())), null, alreadyProcessed);
      if (!innerResult.isEmpty()) {
        result = cartesianProduct(entry.getKey(), result, innerResult);
      }
    }

    for (String key : innerResults.keySet()) {
      limit = initialLimit;
      result = cartesianProduct(key, result, innerResults.get(key));
    }

    return result.stream().filter(v -> !v.tupleValue().isEmpty()).collect(Collectors.toList());
  }

  private List<ExprValue> cartesianProduct(String path, List<ExprValue> acceptor,
                                           List<ExprValue> toMerge) {
    List<ExprValue> result = new ArrayList<>();
    String[] paths = path.split("\\.");

    for (ExprValue acceptorExpr : acceptor) {
      for (ExprValue mergeExpr : toMerge) {
        ExprTupleValue union = ((ExprTupleValue) acceptorExpr).deepCopy();
        ExprTupleValue current = union;
        for (int i = 0; i < paths.length - 1; i++) {
          String p = paths[i];
          if (!current.tupleValue().containsKey(p)) {
            current.tupleValue().put(p, new ExprTupleValue(new LinkedHashMap<>()));
          }
          current = (ExprTupleValue) current.tupleValue().get(p);
        }
        current.tupleValue().put(paths[paths.length - 1], mergeExpr);
        result.add(union);
        if (limit != null) {
          --limit;
        }
        if (limitReached()) {
          break;
        }
      }
      if (limitReached()) {
        break;
      }
    }

    return result;
  }

  private ExprCollectionValue parseArray(Content content, String field) {
    List<ExprValue> result = new ArrayList<>();
    content.array().forEachRemaining(v -> result.addAll(
            parse(v, field, STRUCT, null, Collections.emptySet())
    ));
    return new ExprCollectionValue(result);
  }

  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }

  private boolean limitReached() {
    return limit != null && limit == 0;
  }

  private Integer ratioCeil(Integer a, Integer b) {
    if (a == null) {
      return null;
    }
    return (a + b - 1) / b;
  }
}
