package io.four.raft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.baidu.brpc.server.RpcServer;
import io.four.raft.core.rpc.RaftRemoteServiceImpl;
import io.four.raft.proto.Raft.*;

import static io.four.raft.core.NodeState.*;
import static io.four.raft.core.RemoteNodeClient.*;
import static io.four.raft.core.Utils.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RaftNode {
    private RaftConfig config;
    private Server localServer; // server info
    private List<RemoteNodeClient> cluster;
    private ClusterConfig clusterConfig;
    private NodeState state = NodeState.STATE_FOLLOWER;
    private long term;
    private int leaderId;
    private long commitIndex;
    private long applyIndex;
    private int voteFor;
    private StateMachine stateMachine;

    private Lock lock = new ReentrantLock();

    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService executorService;

    private RpcServer rpcServer;

    public RaftNode(List<Server> servers, Server localServer, StateMachine stateMachine, RaftConfig config) {
        this.cluster = new ArrayList<>();
        this.localServer = localServer;
        servers.stream().filter(e -> e.getServerId() != localServer.getServerId()).forEach(server -> cluster.add(new RemoteNodeClient(server)));
        this.stateMachine = stateMachine;
        this.config = config;
        this.clusterConfig = ClusterConfig.newBuilder().addAllServers(servers).build();
        LOG.info(format(clusterConfig));

        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
        this.rpcServer = new RpcServer(localServer.getHost(), localServer.getPort());
        this.executorService = Executors.newFixedThreadPool(4);
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

    public void startElectionTask() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        long time = round(config.getElectionFrom(), config.getElectionTo());
        electionScheduledFuture = scheduledExecutorService.schedule(() -> preVote(), time, MILLISECONDS);
    }

    void preVote() {
        lock.lock();
        try {
            LOG.info("start preVote");
            if (!inCluster()) {
                startElectionTask();
                return;
            }
            // pre vote
            this.state = STATE_PRE_CANDIDATE;

            for (RemoteNodeClient node : cluster) {
                CompletableFuture.supplyAsync(() -> node.preVote(buildVoteRest()))
                        .orTimeout(config.getElectionTimeout(), MILLISECONDS)
                        .whenCompleteAsync((r, e) -> processPreVoteResp(r, e, term));
            }
            LOG.info("end  preVote");
            startElectionTask();
        } finally {
            lock.unlock();
        }
    }

    private void processPreVoteResp(VoteResponse response, Throwable e, long oldTerm) {
        lock.lock();
        try {
            if (e != null) {
                LOG.error("Pre vote err" + e);
                return;
            }
            LOG.info("pre vote resp {} {}", format(response), state);
            // if pass start vote to be candidate
            if (state != STATE_PRE_CANDIDATE || term != oldTerm) {
                LOG.info("Rec old vote from {} old term {}", response.getServerId(), oldTerm);
                return;
            }
            if (response.getTerm() > term) {
                LOG.info("已经有leader了");
                toFollower();
                return;
            } else {
                vote(response.getServerId(), cluster, response.getGranted());
                // 判断是否获胜出预选
                if (countVote(cluster) > (clusterConfig.getServersList().size()) / 2) {
                    startVote();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void startVote() {
        LOG.info("start vote");

        lock.lock();
        term++;
        state = STATE_CANDIDATE;
        voteFor = localServer.getServerId();
        for (RemoteNodeClient node : cluster) {
            CompletableFuture.supplyAsync(() -> node.vote(buildVoteRest()))
                    .orTimeout(config.getElectionTimeout(), MILLISECONDS)
                    .whenCompleteAsync((r, t) -> processVoteResp(r, term));
        }
        lock.unlock();
        LOG.info("vote over");

    }

    private void processVoteResp(VoteResponse response, long oldTerm) {
        lock.lock();
        LOG.info("Rec vote from {} {}", format(response), oldTerm);
        try {
            if (state != STATE_CANDIDATE || term != oldTerm) {
                LOG.info("Rec old vote from {} {}", format(response), oldTerm);
                return;
            }
            if (response.getTerm() > term) {
                LOG.info("已经有leader了");
                toFollower();
            } else {
                vote(response.getServerId(), cluster, response.getGranted());
                // 判断是否获胜出预选
                if (countVote(cluster) > (clusterConfig.getServersList().size()) / 2) {
                    toLeader();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void toLeader() {
        try {
            state = STATE_LEADER;
            leaderId = localServer.getServerId();
            if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                electionScheduledFuture.cancel(true);
            }
            LOG.info("become leader, start send heart beat to");
            startHeartbeat();
        } catch (Exception e) {
            LOG.info("to leader err ", e);
        }
    }

    public void toFollower() {
        state = STATE_FOLLOWER;
        LOG.info("to follower ,and stop hear beat!");
        startElectionTask();
        if (heartbeatScheduledFuture != null && heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
    }

    private void startHeartbeat() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(() -> heartBeatJob(), config.getHeartbeatTime(), MILLISECONDS);
    }

    void heartBeatJob() {
        lock.lock();
        for (RemoteNodeClient node : cluster) {
            executorService.submit(() -> node.appendEntries(buildPingEntry()));
        }
        startHeartbeat();
        lock.unlock();
    }

    private AppendEntriesRequest buildPingEntry() {
        return AppendEntriesRequest.newBuilder().setTerm(term)
                .setServerId(localServer.getServerId())
                .build();
    }

    VoteRequest buildVoteRest() {
        return VoteRequest.newBuilder().setServerId(localServer.getServerId())
                .setTerm(term)
                .setLastLogTerm(/**log.get**/term)
                .setLastLogIndex(/**log.get_last_index**/0)
                .build();
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

    public RaftNode setTerm(long term) {
        this.term = term;
        return this;
    }

    public int getVoteFor() {
        return voteFor;
    }

    public RaftNode setVoteFor(int voteFor) {
        this.voteFor = voteFor;
        return this;
    }

    public Lock getLock() {
        return lock;
    }

    public NodeState getState() {
        return state;
    }
}
