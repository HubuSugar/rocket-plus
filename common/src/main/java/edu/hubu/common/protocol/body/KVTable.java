package edu.hubu.common.protocol.body;

import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.HashMap;

/**
 * @author: sugar
 * @date: 2023/6/22
 * @description:
 */
public class KVTable extends RemotingSerialize {
    private HashMap<String, String> table = new HashMap<>();

    public HashMap<String, String> getTable() {
        return table;
    }

    public void setTable(HashMap<String, String> table) {
        this.table = table;
    }
}
