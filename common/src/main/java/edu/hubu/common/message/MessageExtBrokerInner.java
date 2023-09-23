package edu.hubu.common.message;

import edu.hubu.common.TopicFilterType;

/**
 * @author: sugar
 * @date: 2023/7/13
 * @description:
 */
public class MessageExtBrokerInner extends MessageExt{
    private String propertiesString;
    private long tagsCode;

    public static long tagsString2tagsCode(final TopicFilterType topicFilterType, final String tags){
        if(tags == null || tags.length() == 0) return 0;
        return tags.hashCode();
    }

    public static long tagsString2tagsCode(final String tags){
        return tagsString2tagsCode(null, tags);
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public String getPropertiesString() {
        return propertiesString;
    }

    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }
}
