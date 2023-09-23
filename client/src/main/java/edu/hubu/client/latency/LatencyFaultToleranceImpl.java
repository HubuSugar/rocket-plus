package edu.hubu.client.latency;

import edu.hubu.client.common.ThreadLocalIndex;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2023/6/4
 * @description:
 */
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String>{

    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>();
    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    @Override
    public void updateFaultItem(String name, long currentLatency, long notAvailableDuration) {
        FaultItem old = faultItemTable.get(name);
        if(old == null){
            FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.setBeginTimestamp(notAvailableDuration + System.currentTimeMillis());

            old = faultItemTable.putIfAbsent(name, faultItem);
            if(old != null){
                old.setCurrentLatency(currentLatency);
                old.setBeginTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        }else{
            old.setCurrentLatency(currentLatency);
            old.setBeginTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    @Override
    public boolean isAvailable(final String name){
        FaultItem faultItem = faultItemTable.get(name);
        if(faultItem != null){
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public String pickOneAtLeast() {
        Enumeration<FaultItem> elements = faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<>();
        while (elements.hasMoreElements()){
            tmpList.add(elements.nextElement());
        }

        if(tmpList.size() > 0){
            Collections.shuffle(tmpList);
            Collections.sort(tmpList);

            int half = tmpList.size() / 2;
            if(half <= 0) {
                return tmpList.get(0).getName();
            }else{
                int i = this.whichItemWorst.getAndIncrement() % half;
                return tmpList.get(i).getName();
            }
        }

        return null;
    }

    @Override
    public void remove(String notBestBroker) {
        faultItemTable.remove(notBestBroker);
    }

    static class FaultItem implements Comparable<FaultItem>{
        private final String name;
        //下一次可用的时间
        private volatile long beginTimestamp;
        private volatile long currentLatency;

        public FaultItem(String name) {
            this.name = name;
        }

        public boolean isAvailable(){
            return (System.currentTimeMillis() - this.beginTimestamp) >= 0;
        }

        @Override
        public int compareTo(FaultItem o) {
            return 0;
        }

        public String getName() {
            return name;
        }

        public long getBeginTimestamp() {
            return beginTimestamp;
        }

        public void setBeginTimestamp(long beginTimestamp) {
            this.beginTimestamp = beginTimestamp;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(long currentLatency) {
            this.currentLatency = currentLatency;
        }
    }
}
