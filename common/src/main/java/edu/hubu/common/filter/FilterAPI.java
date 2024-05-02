package edu.hubu.common.filter;

import edu.hubu.common.protocol.heartbeat.SubscriptionData;

/**
 * @author: sugar
 * @date: 2023/10/30
 * @description:
 */
public class FilterAPI {

    public static SubscriptionData buildSubscriptionData(String consumerGroup, String topic, String subExpression) throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subExpression);

        if(subExpression == null || subExpression.length() == 0 || SubscriptionData.SUB_ALL.equals(subExpression)){
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        }else{
            String[] tags = subExpression.split("\\|\\|");
            if(tags.length > 0){
                for (String tag : tags) {
                    if(tag.trim().length() > 0){
                       subscriptionData.getTagSet().add(tag.trim());
                       subscriptionData.getCodeSet().add(tag.trim().hashCode());
                    }
                }
            }else{
                throw new Exception("sub expression split error");
            }
        }
        return subscriptionData;
    }

    public static SubscriptionData build(final String topic, final String substring, final String type) throws Exception {

        if(ExpressionType.TAG.equals(type) || type == null){
            return buildSubscriptionData(null, topic, substring);
        }

        if(substring == null || substring.length() < 1){
            throw new IllegalArgumentException("Expression can not be null: " + type);
        }
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(substring);
        subscriptionData.setExpressionType(type);
        return subscriptionData;
    }
}
