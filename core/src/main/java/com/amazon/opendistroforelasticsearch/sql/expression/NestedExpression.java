package com.amazon.opendistroforelasticsearch.sql.expression;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import lombok.Getter;

public class NestedExpression extends ReferenceExpression {
  @Getter
  private final String nestedPath;

  public NestedExpression(String ref, String nestedPath, ExprType exprType) {
    super(ref, exprType);
    this.nestedPath = nestedPath;
  }
}