package edu.hubu.common.protocol.heartbeat;

import edu.hubu.common.filter.ExpressionType;
import lombok.EqualsAndHashCode;

import java.util.HashSet;
import java.util.Set;

/**
 * 订阅topic, tag, tag的hash值
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
@EqualsAndHashCode
public class SubscriptionData {
    public static final String SUB_ALL = "*";

    private boolean classFilterMode = false;
    private String topic;
    private String subString;

    private Set<String> tagSet = new HashSet<>();
    private Set<Integer> codeSet = new HashSet<>();
    private long subVersion = System.currentTimeMillis();
    private String expressionType = ExpressionType.TAG;

    private String filterClassSource;


    public SubscriptionData() {
    }

    public SubscriptionData(String topic, String subString) {
        this.topic = topic;
        this.subString = subString;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSubString() {
        return subString;
    }

    public void setSubString(String subString) {
        this.subString = subString;
    }

    public Set<String> getTagSet() {
        return tagSet;
    }

    public void setTagSet(Set<String> tagSet) {
        this.tagSet = tagSet;
    }

    public Set<Integer> getCodeSet() {
        return codeSet;
    }

    public void setCodeSet(Set<Integer> codeSet) {
        this.codeSet = codeSet;
    }

    public boolean isClassFilterMode() {
        return classFilterMode;
    }

    public void setClassFilterMode(boolean classFilterMode) {
        this.classFilterMode = classFilterMode;
    }

    public long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    public String getFilterClassSource() {
        return filterClassSource;
    }

    public void setFilterClassSource(String filterClassSource) {
        this.filterClassSource = filterClassSource;
    }
}
