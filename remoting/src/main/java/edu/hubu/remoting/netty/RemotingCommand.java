package edu.hubu.remoting.netty;

import com.alibaba.fastjson.annotation.JSONField;
import edu.hubu.remoting.netty.annotation.NotNull;
import edu.hubu.remoting.netty.codec.RocketMQSerialize;
import edu.hubu.remoting.netty.codec.SerializeType;
import edu.hubu.remoting.netty.exception.RemotingCommandException;
import edu.hubu.remoting.netty.protocol.LanguageCode;
import edu.hubu.remoting.netty.protocol.RemotingSerialize;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/5/17
 * @description:
 */
@Slf4j
public class RemotingCommand {
    public static final String REMOTING_VERSION_KEY = "rocket.remoting.version";
    private static final AtomicInteger requestId = new AtomicInteger(0);
    private static final int RPC_ONEWAY = 1;
    private static final int RPC_TYPE = 0;
    //缓存CustomHeader子类的属性
    private static final Map<Class<? extends CustomCommandHeader>, Field[]> CLASS_HEADER_MAP = new HashMap<>();
    //缓存每个字段是否有nullable注解
    private static final HashMap<Field, Boolean> NULLABLE_FIELD_MAP = new HashMap<>();
    //存每个字段对应的基本类型
    private static final HashMap<Class, String> CANONICAL_NAME_MAP = new HashMap<>();
    private static final String CANONICAL_STRING_NAME = String.class.getCanonicalName();
    private static final String CANONICAL_INTEGER_NAME = Integer.class.getCanonicalName();
    private static final String CANONICAL_INTEGER_NAME_1 = int.class.getCanonicalName();
    private static final String CANONICAL_LONE_NAME = Long.class.getCanonicalName();
    private static final String CANONICAL_LONE_NAME_1 = long.class.getCanonicalName();
    private static final String CANONICAL_BOOLEAN_NAME = Boolean.class.getCanonicalName();
    private static final String CANONICAL_BOOLEAN_NAME_1 = boolean.class.getCanonicalName();
    private static final String CANONICAL_DOUBLE_NAME = Double.class.getCanonicalName();
    private static final String CANONICAL_DOUBLE_NAME_1 = double.class.getCanonicalName();

    private static SerializeType serializeTypeInThisServer = SerializeType.JSON;
    private int code;
    private LanguageCode language = LanguageCode.JAVA;
    private int version;
    private int opaque = requestId.getAndIncrement();
    private int flag = 0;
    private String remark;
    private static volatile int configVersion = -1;


    private HashMap<String, String> extFields;
    private transient CustomCommandHeader customHeader;
    private transient byte[] body;
    private SerializeType serializeTypeCurrentRPC = serializeTypeInThisServer;


    static {
        serializeTypeInThisServer = SerializeType.valueOf(System.getProperty("serializeType", "JSON"));
    }

    protected RemotingCommand(){

    }

    public static RemotingCommand createRequestCommand(int requestCode, CustomCommandHeader customHeader){
        RemotingCommand command = new RemotingCommand();
        command.setCode(requestCode);
        command.customHeader = customHeader;
        setCmdVersion(command);
        return command;
    }

    public static RemotingCommand createResponseCommand(int code, String remark){
       return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand createResponseCommand(Class<? extends CustomCommandHeader> customHeader){
        return createResponseCommand(ResponseCode.SYSTEM_ERROR, "null response code", customHeader);
    }

    public static RemotingCommand createResponseCommand(int code, String remark, Class<? extends CustomCommandHeader> customHeader){
        RemotingCommand response = new RemotingCommand();
        response.markResponseType();
        response.setCode(code);
        response.setRemark(remark);
        setCmdVersion(response);

        if(customHeader != null){
            try {
                response.customHeader = customHeader.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                return null;
            }
        }
        return response;
    }

    private static void setCmdVersion(RemotingCommand cmd){
        if(configVersion >= 0){
            cmd.setVersion(configVersion);
        }else{
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if(v != null){
                int version = Integer.parseInt(v);
                cmd.setVersion(version);
                configVersion = version;
            }

        }
    }

    public CustomCommandHeader decodeCustomCommandHeader(Class<? extends CustomCommandHeader> clazz){
        CustomCommandHeader objectHeader = null;
        try {
            objectHeader = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            return null;
        }

        if (this.extFields != null) {
            Field[] fields = this.getClazzFields(clazz);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (value == null) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the value of " + fieldName + "can not be null");
                                }
                                continue;
                            }
                            field.setAccessible(true);
                            String name = this.getCanonicalName(field.getType());
                            Object valueParsed;
                            if(CANONICAL_STRING_NAME.equals(name)){
                                valueParsed = value;
                            }else if(CANONICAL_INTEGER_NAME.equals(name) || CANONICAL_INTEGER_NAME_1.equals(name)){
                                valueParsed = Integer.parseInt(value);
                            }else if(CANONICAL_BOOLEAN_NAME.equals(name) || CANONICAL_BOOLEAN_NAME_1.equals(name)){
                                valueParsed = Boolean.parseBoolean(value);
                            }else if(CANONICAL_LONE_NAME.equals(name) || CANONICAL_LONE_NAME_1.equals(name)){
                                valueParsed = Long.parseLong(value);
                            }else if(CANONICAL_DOUBLE_NAME.equals(name) || CANONICAL_DOUBLE_NAME_1.equals(name)){
                                valueParsed = Double.parseDouble(value);
                            }else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);
                        } catch (Exception e) {
                            log.error("parse header exception", e);
                        }
                    }

                }
            }

        }

        return objectHeader;
    }

    public static RemotingCommand decode(ByteBuffer buffer){
        int length = buffer.limit();
        int oriHeaderLength = buffer.getInt();
        int headerLength = getHeaderLength(oriHeaderLength);

        byte[] headerData = new byte[headerLength];
        buffer.get(headerData);

        RemotingCommand command = headerDecode(headerData, getProtocolType(oriHeaderLength));
        int bodyLen = length - 4 -headerLength;
        byte[] body = null;
        if(bodyLen > 0){
            body = new byte[bodyLen];
            buffer.get(body);
        }
        command.body = body;
        return command;
    }

    public static SerializeType getProtocolType(int len){
        byte type = (byte) ((len >> 24) & 0xFF);
        return SerializeType.valueOf(type);
    }

    public static RemotingCommand headerDecode(byte[] headerData, SerializeType type){
        switch (type){
            case JSON:
               RemotingCommand command = RemotingSerialize.decode(headerData, RemotingCommand.class);
               command.serializeTypeCurrentRPC = type;
               return command;
            case ROCKETMQ:  //todo
               RemotingCommand command1 = RocketMQSerialize.decode(headerData);
               command1.serializeTypeCurrentRPC = type;
               return command1;
        }
        return null;
    }

    public static int getHeaderLength(int len){
        return len & 0xFFFFFF;
    }


    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body == null ? 0 : body.length);
    }

    public ByteBuffer encodeHeader(int bodyLength){
        // the header length size
        int length = 4;

        byte[] headerData = headerEncode();

        // header length
        length += headerData.length;

        //body length
        length += bodyLength;

        ByteBuffer buffer = ByteBuffer.allocate(4 + length - bodyLength);

        //total length
        buffer.putInt(length);

        //header length
        buffer.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        //header data
        buffer.put(headerData);

        buffer.flip();

        return buffer;
    }

    private byte[] headerEncode(){
        this.makeCustomHeaderToNet();
        if(SerializeType.ROCKETMQ == this.serializeTypeCurrentRPC){
            return RocketMQSerialize.rocketMQSerialize(this);
        }else{
            return RemotingSerialize.encode(this);
        }
    }

    public void makeCustomHeaderToNet(){
        if(this.customHeader == null) return;
        Field[] fields = this.getClazzFields(this.customHeader.getClass());
        if(this.extFields == null){
            this.extFields = new HashMap<>();
        }
        for (Field field : fields) {
            if(!Modifier.isStatic(field.getModifiers())){
                String name = field.getName();
                if(!name.startsWith("this")){
                    field.setAccessible(true);
                    Object value = null;
                    try {
                        value = field.get(customHeader);

                    } catch (IllegalAccessException e) {
                        log.error("");
                    }

                    if(value != null){
                        extFields.put(name, value.toString());
                    }
                }
            }
        }
    }

    public static byte[] markProtocolType(int length, SerializeType serializeType){
        byte[] result = new byte[4];
        result[0] = serializeType.getCode();
        result[1] = (byte)((length >> 16) & 0xFF);
        result[2] = (byte)((length >> 8) & 0xFF);
        result[3] = (byte)((length & 0xFF));
        return result;
    }

    private Field[] getClazzFields(Class<? extends CustomCommandHeader> clazz){
        Field[] fields = CLASS_HEADER_MAP.get(clazz);
        if(fields == null){
            fields = clazz.getDeclaredFields();
            synchronized (CLASS_HEADER_MAP){
                   CLASS_HEADER_MAP.put(clazz, fields);
            }
        }
        return fields;
    }

    private boolean isFieldNullable(Field field){
        if(!NULLABLE_FIELD_MAP.containsKey(field)){
            synchronized (NULLABLE_FIELD_MAP){
                NotNull annotation = field.getAnnotation(NotNull.class);
                NULLABLE_FIELD_MAP.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_MAP.get(field);
    }

    private String getCanonicalName(Class clazz){
        String name = CANONICAL_NAME_MAP.get(clazz);
        if(name == null){
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_MAP){
                CANONICAL_NAME_MAP.put(clazz, name);
            }
        }
        return name;
    }

    public boolean isOnewayType(){
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits ) == bits;
    }

    public void markResponseType(){
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isResponseType(){
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    @JSONField(serialize = false)
    public CommandType getCommandType() {
        if(isResponseType()){
            return CommandType.RESPONSE;
        }
        return CommandType.REQUEST;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public CustomCommandHeader readCustomHeader() {
        return this.customHeader;
    }

    public void writeCustomHeader(CustomCommandHeader customHeader){
        this.customHeader = customHeader;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + "JAVA" + ", version=" + "471" + ", opaque=" + opaque + ", flag(B)="
                + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
                + serializeTypeCurrentRPC + "]";
    }
}
