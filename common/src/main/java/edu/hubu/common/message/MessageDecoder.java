package edu.hubu.common.message;

import edu.hubu.common.sysFlag.MessageSysFlag;
import edu.hubu.common.utils.UtilAll;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class MessageDecoder {

    public static final char NAME_VALUE_SEPARATOR = 1;
    public static final char PROPERTY_SEPARATOR = 2;
    public static final int SYS_FLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;

    public static List<MessageExt> decodes(ByteBuffer byteBuffer) {
        return decodes(byteBuffer, true);
    }

    public static List<MessageExt> decodes(ByteBuffer byteBuffer, boolean readBody){
        List<MessageExt> messageExts = new ArrayList<>();
        while (byteBuffer.hasRemaining()){
            MessageExt messageExt = clientDecode(byteBuffer, readBody);
            if(messageExt != null){
                messageExts.add(messageExt);
            }else {
                break;
            }
        }
        return messageExts;
    }

    private static MessageExt clientDecode(ByteBuffer byteBuffer, boolean readBody) {
        return decode(byteBuffer, readBody, true, true);
    }

    private static MessageExt decode(ByteBuffer byteBuffer, boolean readBody, boolean deCompress, boolean isClient) {
        try{
            MessageExt messageExt;
            if(isClient){
                messageExt = new MessageClientExt();
            }else{
                messageExt = new MessageExt();
            }

            //1、total size
            int storeSize = byteBuffer.getInt();
            messageExt.setStoreSize(storeSize);

            //2、magic code
            byteBuffer.getInt();

            //3、body crc
            int bodyCRC = byteBuffer.getInt();
            messageExt.setBodyCRC(bodyCRC);

            //4、queueId
            int queueId = byteBuffer.getInt();
            messageExt.setQueueId(queueId);

            //5、flag
            int flag = byteBuffer.getInt();
            messageExt.setFlag(flag);

            //6、queueOffset
            long queueOffset = byteBuffer.getLong();
            messageExt.setQueueOffset(queueOffset);

            //7、physicalOffset
            long physicalOffset = byteBuffer.getLong();
            messageExt.setCommitLogOffset(physicalOffset);

            //8、sysFlag
            int sysFlag = byteBuffer.getInt();
            messageExt.setSysFlag(sysFlag);

            //9、bornTimestamp
            long bornTimestamp = byteBuffer.getLong();
            messageExt.setBornTimestamp(bornTimestamp);

            //10、bornHost
            int bornHostIPLength = (sysFlag & MessageSysFlag.FLAG_BORN_HOST_V6) == 0 ? 4 : 16;
            byte[] bornHostContent = new byte[bornHostIPLength];
            byteBuffer.get(bornHostContent, 0, bornHostIPLength);
            int port = byteBuffer.getInt();
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHostContent), port));

            //11、store timestamp
            long storeTimestamp = byteBuffer.getLong();
            messageExt.setStoreTimestamp(storeTimestamp);

            //12、store host
            int storeHostIPLength = (sysFlag & MessageSysFlag.FLAG_STORE_HOST_V6) == 0 ? 4 : 16;
            byte[] storeHostContent = new byte[storeHostIPLength];
            byteBuffer.get(storeHostContent, 0, storeHostIPLength);
            int storePort = byteBuffer.getInt();
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHostContent), storePort));

            //13、reconsumeTimes
            int reconsumeTimes = byteBuffer.getInt();
            messageExt.setReconsumeTimes(reconsumeTimes);

            //14、preparedTransactionOffset
            long prepareTransactionOffset = byteBuffer.getLong();
            messageExt.setPreparedTransactionOffset(prepareTransactionOffset);

            //15、body
            int bodyLen = byteBuffer.getInt();
            if(bodyLen > 0){
                if(readBody){
                    byte[] body = new byte[bodyLen];
                    byteBuffer.get(body);

                    //uncompress body
                    if(deCompress && (sysFlag & MessageSysFlag.FLAG_COMPRESSED) == MessageSysFlag.FLAG_COMPRESSED){
                        body = UtilAll.unCompress(body);
                    }
                    messageExt.setBody(body);
                }else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }

            }

            //16、topic
            byte topicLen = byteBuffer.get();
            byte[] topicContent = new byte[(int)topicLen];
            byteBuffer.get(topicContent);
            messageExt.setTopic(new String(topicContent, StandardCharsets.UTF_8));

            //17、properties
            short propLength = byteBuffer.getShort();
            if(propLength > 0){
                byte[] propContent = new byte[propLength];
                byteBuffer.get(propContent);
                String properties = new String(propContent, StandardCharsets.UTF_8);
                Map<String, String> props = string2Properties(properties);
                messageExt.setProperties(props);
            }

            //msgId
            int msgIdLength = storeHostIPLength + 4 + 8;
            ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLength);
            String msgId = createdMsgId(msgIdBuffer, messageExt.getStoreHostBytes(), messageExt.getCommitLogOffset());
            messageExt.setMsgId(msgId);

            if(isClient){
                ((MessageClientExt)messageExt).setOffsetMsgId(msgId);
            }
            return messageExt;
        }catch (Exception e){
            byteBuffer.position(byteBuffer.limit());
        }
        return null;
    }

    public static String properties2String(Map<String, String> properties){
        StringBuilder sb = new StringBuilder();
        if(properties == null) return sb.toString();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if(value == null){
                continue;
            }
            sb.append(key).append(NAME_VALUE_SEPARATOR).append(value).append(PROPERTY_SEPARATOR);
        }
        return sb.toString();
    }

    public static Map<String, String> string2Properties(String properties){
        Map<String, String> map = new HashMap<>();
        if(properties == null || properties.length() == 0){
            return map;
        }
        String[] items = properties.split(String.valueOf(PROPERTY_SEPARATOR));
        for (String item : items) {
            String[] i = item.split(String.valueOf(NAME_VALUE_SEPARATOR));
            if(i.length == 2){
                map.put(i[0], i[1]);
            }
        }
        return map;
    }

    /**
     * 生成消息id
     * @param input 消息id的内存变量
     * @param byteBuffer 消息存储机器的地址byteBuffer
     * @param wroteOffset 消息写入commitlog的偏移量
     * @return
     */
    public static String createdMsgId(ByteBuffer input, ByteBuffer byteBuffer, long wroteOffset) {
        input.flip();
        int msgIdLength = byteBuffer.limit() == 8 ? 16 : 28;
        input.limit(msgIdLength);

        input.put(byteBuffer);
        input.putLong(wroteOffset);
        return UtilAll.byte2String(input.array());
    }
}
