package edu.hubu.common.protocol.header.response;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/6/29
 * @description:
 */
public class QueryDataVersionResponseHeader implements CustomCommandHeader {

    private Boolean changed;

    public Boolean getChanged() {
        return changed;
    }

    public void setChanged(Boolean changed) {
        this.changed = changed;
    }

    @Override
    public boolean checkFields() {
        return false;
    }

    @Override
    public String toString() {
        return "QueryDataVersionResponseHeader{" +
                "changed=" + changed +
                '}';
    }
}
