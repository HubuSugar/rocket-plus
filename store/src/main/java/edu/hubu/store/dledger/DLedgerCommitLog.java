package edu.hubu.store.dledger;

import edu.hubu.store.CommitLog;
import edu.hubu.store.DefaultMessageStore;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class DLedgerCommitLog extends CommitLog {

    public DLedgerCommitLog(final DefaultMessageStore messageStore) {
        super(messageStore);

    }
}
