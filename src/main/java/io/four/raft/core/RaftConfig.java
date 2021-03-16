package io.four.raft.core;

import lombok.Data;

@Data
public class RaftConfig {

    // Milliseconds
    private int electionFrom = 4000;
    private int electionTo = 5000;

    private int heartbeatTime = 500;

    private String dir = "/tmp/raft/";

    // 选举rpc超时
    private int electionTimeout = 5000;

    private long maxFileSize = 1024*1024;

    private int maxAppendLogs = Integer.MAX_VALUE;

    private int appendTimeOut = 5000;

}
