package edu.hubu.client.consumer;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class MessageSelector {

    private final String type;
    private final String expression;

    public MessageSelector(String type, String expression) {
        this.type = type;
        this.expression = expression;
    }

    public String getType() {
        return type;
    }

    public String getExpression() {
        return expression;
    }
}
