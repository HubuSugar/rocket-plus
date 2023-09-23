package edu.hubu.store.index;

import edu.hubu.store.DefaultMessageStore;
import edu.hubu.store.consumeQueue.DispatchRequest;

/**
 * @author: sugar
 * @date: 2023/8/19
 * @description:
 */
public class IndexService {

    private final DefaultMessageStore messageStore;

    public IndexService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public void start(){

    }

    public void buildIndex(DispatchRequest request){

    }
}
