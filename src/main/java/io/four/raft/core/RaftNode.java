package io.four.raft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import com.baidu.brpc.server.RpcServer;
import io.four.raft.core.rpc.RaftRemoteServiceImpl;
import io.four.raft.proto.Raft.*;

import static io.four.raft.core.NodeState.*;
import static io.four.raft.core.RemoteNode.*;
import static io.four.raft.core.Utils.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RaftNode {
    private RaftConfig config;
    // server info
    private Server localServer;
    private List<RemoteNode> cluster;
    private ClusterConfig clusterConfig;
    private NodeState state = NodeState.STATE_FOLLOWER;
    private long term;
    private int leaderId;
    private long commitIndex;
    private long applyIndex;
    private int voteFor;
    private StateMachine stateMachine;

    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    private ScheduledExecutorService scheduledExecutorService;

    private RpcServer rpcServer;

    public RaftNode(List<Server> servers, Server localServer, StateMachine stateMachine, RaftConfig config) {
        this.cluster = new ArrayList<>();
        this.localServer = localServer;
        servers.stream().filter(e -> e.getServerId() != localServer.getServerId()).forEach(server -> cluster.add(new RemoteNode(server)));
        this.stateMachine = stateMachine;
        this.config = config;
        this.clusterConfig = ClusterConfig.newBuilder().addAllServers(servers).build();
        LOG.info(clusterConfig.toString());

        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
        this.rpcServer = new RpcServer(localServer.getHost(), localServer.getPort());

    }

    public RaftNode(String servers, String localServer, StateMachine stateMachine) {
        this(parseServers(servers), parseServer(localServer), stateMachine, new RaftConfig());
    }

    public void init() {
        this.rpcServer.registerService(stateMachine);
        this.rpcServer.registerService(new RaftRemoteServiceImpl(this));
        rpcServer.start();
        startElectionTask();
    }

    void startElectionTask() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        electionScheduledFuture = scheduledExecutorService.schedule(
                () -> preVote(), round(config.getElectionFrom(), config.getElectionTo()), MILLISECONDS
        );
    }

    void preVote() {
        System.out.println("start ele");
        if (!inCluster()) {
            startElectionTask();
            return;
        }
        // pre vote
        this.state = STATE_PRE_CANDIDATE;
        for (RemoteNode node : cluster) {
            CompletableFuture.supplyAsync(() -> node.preVote(buildVoteRest()))
                    .orTimeout(config.getElectionTimeout(), MILLISECONDS)
                    .whenCompleteAsync((r, e) -> processPreVoteResp(r, e, term));
        }
        //startElectionTask();
    }


    VoteRequest buildVoteRest() {
        return VoteRequest.newBuilder().setServerId(localServer.getServerId())
                .setTerm(term)
                .setLastLogTerm(/**log.get**/term)
                .setLastLogIndex(/**log.get_last_index**/0)
                .build();
    }

    private void processPreVoteResp(VoteResponse response, Throwable e, long oldTerm) {
        if (e != null) {
            LOG.error("Pre vote err" + e);
            return;
        }
        LOG.info("pre vote resp:\n " + response.toString());
        // if pass start vote to be candidate
        if (state != STATE_PRE_CANDIDATE || term != oldTerm) {
            LOG.info("Rec pre vote from {} old term {}", response, oldTerm);
            return;
        }
        if (response.getTerm() > term) {
            System.out.println("已经有leader了");
            toFollower();
            return;
        } else {
            vote(response.getServerId(), cluster, response.getGranted());
            // 判断是否获胜出预选
            if (countVote(cluster) > (clusterConfig.getServersList().size()) / 2) {
                startVote();
            }
        }
    }

    private void startVote() {
        term++;
        state = STATE_CANDIDATE;
        voteFor = localServer.getServerId();
        for (RemoteNode node : cluster) {
            CompletableFuture.supplyAsync(() -> node.vote(buildVoteRest()))
                    .orTimeout(config.getElectionTimeout(), MILLISECONDS)
                    .whenCompleteAsync((r, t) -> processVoteResp(r, term));
        }
    }

    private void processVoteResp(VoteResponse response, long oldTerm) {
        if (state != STATE_CANDIDATE || term != oldTerm) {
            LOG.info("Rec vote from {} old term {}", response, oldTerm);
            return;
        }
        if (response.getTerm() > term) {
            System.out.println("已经有leader了");
            toFollower();
            return;
        } else {
            vote(response.getServerId(), cluster, response.getGranted());
            // 判断是否获胜出预选
            if (countVote(cluster) > (clusterConfig.getServersList().size()) / 2) {
                toLeader();
            }
        }
    }

    private void toLeader() {
        state = STATE_LEADER;
        leaderId = localServer.getServerId();
        if (electionScheduledFuture != null && electionScheduledFuture.isCancelled()) {
            electionScheduledFuture.cancel(true);
        }
        System.out.println("become leader");
        startHeartbeat();
    }

    private void startHeartbeat() {
    }

    public void toFollower() {
        state = STATE_FOLLOWER;
        System.out.println("to follower ,and stop hear beat!");
        startElectionTask();
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

    private boolean inCluster() {
        return clusterConfig.getServersList().contains(localServer);
    }

    public boolean inCluster(int server_id) {
        for (Server server : clusterConfig.getServersList()) {
            if (server.getServerId() == server_id) {
                return true;
            }
        }
        return false;
    }

    public Server getLocalServer() {
        return localServer;
    }

    public long getTerm() {
        return term;
    }
    public int voteFor() {
        return voteFor;
    }

}
