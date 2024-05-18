package edu.hubu.broker.longpolling;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: sugar
 * @date: 2024/5/4
 * @description:
 */
public class ManyPullRequest {

    private final ArrayList<PullRequest> pullRequests = new ArrayList<>();

    public synchronized void addPullRequest(final PullRequest request){
        this.pullRequests.add(request);
    }

    public synchronized void addPullRequest(final List<PullRequest> requests){
        this.pullRequests.addAll(requests);
    }

    public synchronized List<PullRequest> cloneListAndClear() {
        if(!this.pullRequests.isEmpty()){
            ArrayList<PullRequest> newRequests = (ArrayList<PullRequest>) this.pullRequests.clone();
            this.pullRequests.clear();
            return newRequests;
        }
        return null;
    }
}
