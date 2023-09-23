package edu.hubu.common.protocol.result;

import edu.hubu.common.protocol.body.KVTable;

/**
 * @author: sugar
 * @date: 2023/6/11
 * @description:
 */
public class RegisterBrokerResult {
    private String haServer;
    private String masterAddr;
    private KVTable kvTable;

    public String getHaServer() {
        return haServer;
    }

    public void setHaServer(String haServer) {
        this.haServer = haServer;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    public KVTable getKvTable() {
        return kvTable;
    }

    public void setKvTable(KVTable kvTable) {
        this.kvTable = kvTable;
    }
}
