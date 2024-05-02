package edu.hubu.store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/12/9
 * @description:
 */
public interface MessageFilter {

    boolean isMatchedByConsumeQueue(final Long tagsCode, final ConsumeQueueExt.CqUnitExt cqUnitExt);

    boolean isMatchedByCommitLog(final ByteBuffer byteBuffer, final Map<String,String> properties);
}
