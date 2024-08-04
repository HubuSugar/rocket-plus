package edu.hubu.common.message;

/**
 * @author: sugar
 * @date: 2024/8/4
 * @description:
 */
public class MessageClientExt extends MessageExt{

    public String getOffsetMsgId(){
        return super.getMsgId();
    }

    public void setOffsetMsgId(String offsetMsgId){
        super.setMsgId(offsetMsgId);
    }

    @Override
    public String getMsgId() {
        String uniqID = MessageClientIDSetter.getUniqID(this);
        if(uniqID == null){
            return getOffsetMsgId();
        }else {
            return uniqID;
        }
    }

    @Override
    public void setMsgId(String msgId) {
    }
}
