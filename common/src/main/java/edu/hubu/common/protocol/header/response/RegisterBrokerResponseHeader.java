package edu.hubu.common.protocol.header.response;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/6/11
 * @description:
 */
public class RegisterBrokerResponseHeader implements CustomCommandHeader {

    private String haServer;
    private String masterAddress;

    public String getHaServer() {
        return haServer;
    }

    public void setHaServer(String haServer) {
        this.haServer = haServer;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    @Override
    public boolean checkFields() {
        return false;
    }
}
