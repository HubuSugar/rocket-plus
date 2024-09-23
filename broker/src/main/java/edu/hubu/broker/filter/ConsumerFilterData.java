package edu.hubu.broker.filter;

import edu.hubu.filter.expression.Expression;
import edu.hubu.filter.util.BloomFilterData;

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

    private long bornTime = 0;
    private long deadTime = 0;
    private BloomFilterData bloomFilterData;
    private long clientVersion;

    public boolean isMsgInLive(long msgStoreTime){
        return msgStoreTime > getBornTime();
    }

    public String getConsumeGroup() {
        return consumeGroup;
    }

    public void setConsumeGroup(String consumeGroup) {
        this.consumeGroup = consumeGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    public Expression getCompiledExpression() {
        return compiledExpression;
    }

    public void setCompiledExpression(Expression compiledExpression) {
        this.compiledExpression = compiledExpression;
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(long bornTime) {
        this.bornTime = bornTime;
    }

    public long getDeadTime() {
        return deadTime;
    }

    public void setDeadTime(long deadTime) {
        this.deadTime = deadTime;
    }

    public BloomFilterData getBloomFilterData() {
        return bloomFilterData;
    }

    public void setBloomFilterData(BloomFilterData bloomFilterData) {
        this.bloomFilterData = bloomFilterData;
    }

    public long getClientVersion() {
        return clientVersion;
    }

    public void setClientVersion(long clientVersion) {
        this.clientVersion = clientVersion;
    }
}
