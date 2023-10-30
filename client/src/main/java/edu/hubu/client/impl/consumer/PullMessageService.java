package edu.hubu.client.impl.consumer;

import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.ServiceThread;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
@Slf4j
public class PullMessageService extends ServiceThread {

    private final BlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<>();
    private final MQClientInstance mqClientInstance;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "pullMessageServiceScheduledThread");
        }
    });


    public PullMessageService(MQClientInstance mqClientInstance) {
        this.mqClientInstance = mqClientInstance;
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

    private void pullMessage(final PullRequest pullRequest){
        // final MQConsumerInner consumerInner = this.mqClientInstance.selectConsumer(pullRequest.getConsumerGroup());
    }

    @Override
    public void run() {
        log.info("Pull Message Service started");

        while (!isStopped()){
            try {
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            }catch (Exception e){
                log.error("pull message service run failed", e);
            }

        }

        log.info("Pull Message service stooped");
    }
}
