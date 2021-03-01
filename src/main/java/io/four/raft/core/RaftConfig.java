package io.four.raft.core;

import lombok.Data;

@Data
public class RaftConfig {

    //Milliseconds
    private int electionFrom = 4000;
    private int electionTo = 5000;

    private int heartbeatTime = 500;

    private String dir;
}
