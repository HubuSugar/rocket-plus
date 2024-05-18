package edu.hubu.broker.longpolling;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.ServiceThread;
import edu.hubu.common.SystemClock;
import edu.hubu.store.ConsumeQueueExt;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
@Slf4j
public class PullRequestHoldService extends ServiceThread {

    private static final String TOPIC_QUEUE_ID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock clock = new SystemClock();
    private ConcurrentMap<String/*topic@queueId*/, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<>(1024);

    public PullRequestHoldService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(final PullRequest pullRequest, final String topic, final Integer queueId){
        String pullKey = this.buildKey(topic, queueId);
        ManyPullRequest manyPullRequest = this.pullRequestTable.get(pullKey);
        if(manyPullRequest == null){
            manyPullRequest = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(pullKey, manyPullRequest);
            if(prev != null){
                manyPullRequest = prev;
            }
        }

        manyPullRequest.addPullRequest(pullRequest);
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while(!this.isStopped()){
            try{
                if(this.brokerController.getBrokerConfig().isLongPollingEnable()){
                    this.waitForRunning(5 * 1000);
                }else{
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMillis());
                }
                long now = this.clock.now();
                this.checkHoldRequest();
                long costTime = this.clock.now() - now;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFY]check pull request cost {}ms", costTime);
                }
            }catch (Throwable e){
                log.error("{} service has exception", this.getServiceName());
            }
        }
        log.info("{} service stopped", getServiceName());
    }

    public void checkHoldRequest(){
        for (Map.Entry<String, ManyPullRequest> entry : this.pullRequestTable.entrySet()) {
            String key = entry.getKey();
            String[] keys = key.split(TOPIC_QUEUE_ID_SEPARATOR);
            if(keys.length == 2){
                String topic = keys[0];
                int queueId = Integer.parseInt(keys[1]);
                long maxOffsetInQueue = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try{
                    this.notifyMessageArriving(topic, queueId, maxOffsetInQueue);
                }catch (Throwable e){
                    log.error("check hold request failed, queueId={}, topic={}", queueId, topic, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffsetInQueue){
        this.notifyMessageArriving(topic, queueId, maxOffsetInQueue, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
                                      final long msgStoreTime, byte[] filterMap, Map<String, String> props){
        String key = this.buildKey(topic, queueId);
        ManyPullRequest pullRequest = this.pullRequestTable.get(key);
        if(pullRequest != null){
           List<PullRequest> requests = pullRequest.cloneListAndClear();
           if(requests != null){
               List<PullRequest> replayList = new ArrayList<>();
               for (PullRequest request : requests) {
                   long newestOffset = maxOffset;
                   if(newestOffset <= request.getPullFromThisOffset()){
                       newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                   }
                   if(newestOffset > request.getPullFromThisOffset()){
                       boolean matched = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                               new ConsumeQueueExt.CqUnitExt(tagsCode, msgStoreTime, filterMap));
                       //filter by bit map
                       if(matched && props != null){
                           matched = request.getMessageFilter().isMatchedByCommitLog(null, props);
                       }

                       if(matched){
                          try{
                              this.brokerController.getPullMessageProcessor().executeRequestWhenWakeUp(request.getChannel(),
                                      request.getRemotingCommand());
                          }catch (Exception e){
                              log.error("execute request when wakeup failed", e);
                          }
                          continue;
                       }
                   }
                   //如果hold时间过长的话
                   if(System.currentTimeMillis() >= (request.getSuspendTimeMillis() + request.getTimeoutMillis())){
                       try{
                           this.brokerController.getPullMessageProcessor().executeRequestWhenWakeUp(request.getChannel(),
                                   request.getRemotingCommand());
                       }catch (Exception e){
                           log.error("execute request when wakeup failed", e);
                       }
                       continue;
                   }
                   //重新放回来的request
                   replayList.add(request);
               }

               if(!replayList.isEmpty()){
                   pullRequest.addPullRequest(replayList);
               }
           }
        }
    }

    private String buildKey(String topic, Integer queueId){
        return topic + TOPIC_QUEUE_ID_SEPARATOR + queueId;
    }
}
