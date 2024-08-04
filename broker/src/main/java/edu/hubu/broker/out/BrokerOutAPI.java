package edu.hubu.broker.out;

import edu.hubu.common.DataVersion;
import edu.hubu.common.utils.UtilAll;
import edu.hubu.remoting.netty.exception.RemotingConnectException;
import edu.hubu.remoting.netty.exception.RemotingSendRequestException;
import edu.hubu.remoting.netty.exception.RemotingTimeoutException;
import edu.hubu.common.exception.broker.MQBrokerException;
import edu.hubu.common.protocol.body.KVTable;
import edu.hubu.common.protocol.body.RegisterBrokerBody;
import edu.hubu.common.protocol.body.TopicConfigSerializeWrapper;
import edu.hubu.common.protocol.header.request.QueryDataVersionRequestHeader;
import edu.hubu.common.protocol.header.request.RegisterBrokerRequestHeader;
import edu.hubu.common.protocol.header.response.QueryDataVersionResponseHeader;
import edu.hubu.common.protocol.header.response.RegisterBrokerResponseHeader;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.common.protocol.result.RegisterBrokerResult;
import edu.hubu.remoting.netty.*;
import edu.hubu.remoting.netty.handler.RpcHook;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/6/22
 * @description:
 */
@Slf4j
public class BrokerOutAPI {
    private final RemotingClient remotingClient;
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(32), new ThreadFactory() {
        final AtomicInteger threadIndex = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, String.format("brokerOutAPIThread_%s", threadIndex.getAndIncrement()));
        }
    });

    public BrokerOutAPI(final NettyClientConfig nettyClientConfig){
        this(nettyClientConfig, null);
    }

    public BrokerOutAPI(final NettyClientConfig nettyClientConfig, final RpcHook rpcHook){
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRpcHook(rpcHook);
    }

    //启动netty客户端
    public void start(){
        this.remotingClient.start();
    }


    public void updateNameSrvAddressList(String nameSrvAddress) {
        String[] split = nameSrvAddress.split(";");
        List<String> address = new LinkedList<>(Arrays.asList(split));
        this.remotingClient.updateNameSrvAddress(address);
    }

    /**
     * 通过nameSrv判断是否需要重新注册broker
     * 去nameSrv查询DataVersion
     * @return
     */
    public List<Boolean> needRegister(String clusterName, String brokerAddress, String brokerName, long brokerId,
                                      TopicConfigSerializeWrapper configSerializeWrapper, int registerBrokerTimeoutMillis) {
        final List<Boolean> changeList = new CopyOnWriteArrayList<>();
        List<String> nameSrvList = this.remotingClient.getNameSrvList();
        if(nameSrvList == null || nameSrvList.isEmpty()){
            return changeList;
        }
        final CountDownLatch countDownLatch = new CountDownLatch(nameSrvList.size());
        for (String nameSrv : nameSrvList) {
            executor.execute(() -> {
                try {
                    QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
                    requestHeader.setClusterName(clusterName);
                    requestHeader.setBrokerAddress(brokerAddress);
                    requestHeader.setBrokerName(brokerName);
                    requestHeader.setBrokerId(brokerId);
                    RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
                    request.setBody(configSerializeWrapper.getDataVersion().encode());
                    RemotingCommand response = this.remotingClient.invokeSync(nameSrv, request, registerBrokerTimeoutMillis);

                    DataVersion nameSrvDataVersion = null;
                    Boolean changed = false;
                    switch (response.getCode()){
                        case ResponseCode.SUCCESS:
                            QueryDataVersionResponseHeader queryDataVersionResponseHeader =
                                    (QueryDataVersionResponseHeader)response.decodeCustomCommandHeader(QueryDataVersionResponseHeader.class);
                            changed = queryDataVersionResponseHeader.getChanged();
                            byte[] body = response.getBody();
                            if(body != null){
                                nameSrvDataVersion = DataVersion.decode(body, DataVersion.class);
                                if(!configSerializeWrapper.getDataVersion().equals(nameSrvDataVersion)){
                                    changed = true;
                                }
                            }
                            if(changed == null || changed){
                                changeList.add(Boolean.TRUE);
                            }
                        default:
                            break;
                    }
                    log.warn("query data version from name server {} OK, changed {}, broker {}, name server {}", nameSrv, changed, configSerializeWrapper.getDataVersion(), nameSrvDataVersion == null ? "": nameSrvDataVersion);
                } catch (Exception e) {
                    changeList.add(Boolean.TRUE);
                    log.error("query data version from nameSrv {} exception", nameSrv, e);
                }finally {
                    countDownLatch.countDown();
                }
            });
        }
        try{
            countDownLatch.await(registerBrokerTimeoutMillis, TimeUnit.MILLISECONDS);
        }catch (InterruptedException e){
            log.error("query data version from name server countdown await exception", e);
        }
        return changeList;
    }

    public List<RegisterBrokerResult> registerBrokerAll(String brokerClusterName, String brokerAddr, String brokerName, long brokerId, String haServerAddr,
                                                        TopicConfigSerializeWrapper topicConfigWrapper, List<String> filterServerList,
                                                        boolean oneway, int timeoutMillis, boolean compressed) {
        List<RegisterBrokerResult> result = new ArrayList<>();
        List<String> nameSrvList = this.remotingClient.getNameSrvList();
        if(nameSrvList == null || nameSrvList.isEmpty()){
            return result;
        }
        final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
        requestHeader.setClusterName(brokerClusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerAddress(brokerAddr);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setHaServerAddr(haServerAddr);
        requestHeader.setCompressed(compressed);

        RegisterBrokerBody requestBody = new RegisterBrokerBody();
        requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
        requestBody.setFilterServerList(filterServerList);
        final byte[] body = requestBody.encode(compressed);

        requestHeader.setCrc32(UtilAll.crc32(body));
        final CountDownLatch countDownLatch = new CountDownLatch(nameSrvList.size());
        for (String nameSrv : nameSrvList) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try{
                        RegisterBrokerResult registerBrokerResult = registerBroker(nameSrv, oneway, timeoutMillis, requestHeader, body);
                        if(registerBrokerResult != null){
                            result.add(registerBrokerResult);
                        }
                        log.info("register broker: {} to name server: {}", brokerAddr, nameSrv);
                    }catch (Exception e){
                        log.error("register broker exception", e);
                    }finally {
                        countDownLatch.countDown();
                    }
                }
            });
        }

        try{
            countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
           log.error("register broker await countdown exception", e);
        }

        return result;
    }

    public RegisterBrokerResult registerBroker(String address, boolean oneway, long timeoutMillis, RegisterBrokerRequestHeader requestHeader, byte[] registerBody)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);
        requestCommand.setBody(registerBody);

        if(oneway){
            try{
                this.remotingClient.invokeOneway(address, requestCommand, timeoutMillis);
            }catch (Exception ignored){

            }
            return null;
        }

        RemotingCommand response = this.remotingClient.invokeSync(address, requestCommand, timeoutMillis);
        assert response != null;

        switch (response.getCode()){
            case ResponseCode.SUCCESS:
                RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.decodeCustomCommandHeader(RegisterBrokerResponseHeader.class);
                RegisterBrokerResult registerBrokerResult = new RegisterBrokerResult();
                registerBrokerResult.setMasterAddr(responseHeader.getMasterAddress());
                registerBrokerResult.setHaServer(responseHeader.getHaServer());
                if(response.getBody() != null){
                    registerBrokerResult.setKvTable(KVTable.decode(response.getBody(), KVTable.class));
                }

                return registerBrokerResult;
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }
}
