package edu.hubu.broker.filter;

import edu.hubu.filter.expression.Expression;

/**
 * filter data of consumer
 * @author: sugar
 * @date: 2023/12/3
 * @description:
 */
public class ConsumerFilterData {
    private String consumeGroup;
    private String topic;
    private String expression;
    private String expressionType;
    private transient Expression compiledExpression;

}
