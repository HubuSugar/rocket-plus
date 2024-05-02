package edu.hubu.store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/12/9
 * @description:
 */
public class DefaultMessageFilter implements MessageFilter{

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqUnitExt cqUnitExt) {
        return false;
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer byteBuffer, Map<String, String> properties) {
        return false;
    }
}
