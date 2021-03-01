package io.four.raft.core;

import io.four.raft.proto.Raft.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.four.raft.core.Utils.round;

public class RaftNode {
    private RaftConfig config;
    // server info
    private Server localServer;
    private List<RemoteNode> cluster;
    private NodeState state = NodeState.STATE_FOLLOWER;
    private long term;
    private int leaderId;
    private long commitIndex;
    private long applyIndex;
    private StateMachine stateMachine;

    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    private ScheduledExecutorService scheduledExecutorService;

    public RaftNode(List<Server> servers, Server localServer, StateMachine stateMachine, RaftConfig config) {
        this.cluster = new ArrayList<>();
        this.localServer = localServer;
        servers.forEach(server -> cluster.add(new RemoteNode(server)));
        this.stateMachine = stateMachine;
        this.config = config;
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
    }

    public RaftNode(List<Server> servers, Server localServer, StateMachine stateMachine) {
        this(servers, localServer, stateMachine, new RaftConfig());
    }

    public void init() {

    }

    void electionTask() {
        // pre vote
        // if pass start vote to candidate
        // change to leader or follower
    }
    void startElectionTask() {
        if(electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        electionScheduledFuture = scheduledExecutorService.schedule(
                () -> electionTask(), round(config.getElectionFrom(), config.getElectionTo()), TimeUnit.MILLISECONDS
        );
    }

    void heartBeatJob() {
        //
    }

    void appendLog() {
        // rpc
        // check behind ? catch up
        // commit log
    }

    // client
    public boolean clientApply() {
        return true;
    }


}
