package io.four.raft.core;

public class RaftConfig {

    //Milliseconds
    private int electionTimeout = 5000;

    private int heartbeatTime = 500;

    private String dir;
}
