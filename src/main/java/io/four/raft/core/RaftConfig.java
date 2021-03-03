package io.four.raft.core;

import lombok.Data;

@Data
public class RaftConfig {

    //Milliseconds
    private int electionFrom = 4000*2;
    private int electionTo = 5000*2;

    private int heartbeatTime = 500;

    private String dir;

    /**
     * 选举rpc超时
     */
    private int electionTimeout = 5000;
}
