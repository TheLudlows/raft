package io.four.raft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.baidu.brpc.server.RpcServer;
import com.google.protobuf.ByteString;
import io.four.raft.core.log.RaftLog;
import io.four.raft.core.rpc.RaftRemoteServiceImpl;
import io.four.raft.proto.Raft.*;
import org.tinylog.Logger;

import static io.four.raft.core.Node.NodeState.*;
import static io.four.raft.core.RemoteNode.*;
import static io.four.raft.core.Utils.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RaftNode extends Node {
    private RaftConfig config;
    private List<RemoteNode> others;
    private ClusterConfig clusterConfig;
    private StateMachine stateMachine;
    protected int leaderId;

    private Lock lock = new ReentrantLock();

    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService executorService;
    private RpcServer rpcServer;
    protected RaftLog raftLog;

    public RaftNode(List<Server> servers, Server serverInfo, StateMachine stateMachine, RaftConfig config) {
        try {
            this.others = new ArrayList<>();
            this.serverInfo = serverInfo;
            servers.stream().filter(e -> e.getServerId() != serverInfo.getServerId())
                    .forEach(server -> others.add(new RemoteNode(server)));
            this.stateMachine = stateMachine;
            this.config = config;
            this.clusterConfig = ClusterConfig.newBuilder().addAllServers(servers).build();
            this.scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
            this.rpcServer = new RpcServer(serverInfo.getHost(), serverInfo.getPort());
            this.executorService = Executors.newFixedThreadPool(4);
            this.raftLog = new RaftLog(config.getDir() + serverInfo.getServerId(), config.getMaxFileSize());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public RaftNode(String servers, String serverInfo, StateMachine stateMachine) {
        this(parseServers(servers), parseServer(serverInfo), stateMachine, new RaftConfig());
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
            Logger.info("start preVote");
            if (!inCluster()) {
                startElectionTask();
                return;
            }
            // pre vote
            this.state = STATE_PRE_CANDIDATE;
            resetCluster(others);
            for (RemoteNode node : others) {
                CompletableFuture.supplyAsync(() -> node.preVote(buildVoteRest()))
                        .orTimeout(config.getElectionTimeout(), MILLISECONDS)
                        .whenCompleteAsync((r, e) -> processPreVoteResp(r, e, term));
            }
            Logger.info("end  preVote");
            startElectionTask();
        } finally {
            lock.unlock();
        }
    }

    private void processPreVoteResp(VoteResponse response, Throwable e, long oldTerm) {
        lock.lock();
        try {
            Logger.info("pre vote resp {} {}", format(response), state);
            // if pass start vote to be candidate
            if (state != STATE_PRE_CANDIDATE || term != oldTerm) {
                Logger.info("Rec old vote from {} old term {}", response.getServerId(), oldTerm);
                return;
            }
            if (response.getTerm() > term) {
                toFollower(response.getTerm());
                return;
            } else {
                if (response.getGranted()) {
                    vote(response.getServerId(), others, serverInfo.getServerId());
                }
                // 判断是否获胜出预选
                if (countVote(others, serverInfo.getServerId()) > (clusterConfig.getServersList().size()) / 2) {
                    startVote();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void startVote() {
        Logger.info("start vote");
        lock.lock();
        term++;
        state = STATE_CANDIDATE;
        voteFor = serverInfo.getServerId();
        resetCluster(others);
        for (RemoteNode node : others) {
            CompletableFuture.supplyAsync(() -> node.vote(buildVoteRest()))
                    .orTimeout(config.getElectionTimeout(), MILLISECONDS)
                    .whenCompleteAsync((r, t) -> processVoteResp(r, term));
        }
        lock.unlock();
        Logger.info("vote over");

    }

    private void processVoteResp(VoteResponse response, long oldTerm) {
        lock.lock();
        Logger.info("Rec vote from {} {}", format(response), oldTerm);
        try {
            if (state != STATE_CANDIDATE || term != oldTerm) {
                Logger.info("Rec old vote from {} {}", format(response), oldTerm);
                return;
            }
            if (response.getTerm() > term) {
                Logger.info("已经有leader了");
                toFollower(response.getTerm());
            } else {
                if (response.getGranted()) {
                    vote(response.getServerId(), others, serverInfo.getServerId());
                }
                // 判断是否获胜出预选
                if (countVote(others, serverInfo.getServerId()) > (clusterConfig.getServersList().size()) / 2) {
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
            leaderId = serverInfo.getServerId();
            if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                electionScheduledFuture.cancel(true);
            }
            Logger.info("Become leader{}", this.toString());
            startHeartbeat();
        } catch (Exception e) {
            Logger.info("to leader err ", e);
        }
    }

    public void toFollower(long term) {
        state = STATE_FOLLOWER;
        this.term = term;
        Logger.info("To follower {}", this.toString());
        startElectionTask();
        if (heartbeatScheduledFuture != null && heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
    }

    public void toFollower(long term, int leaderId) {
        this.voteFor = leaderId;
        toFollower(term);
    }


    private void startHeartbeat() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(() -> heartBeatJob(), config.getHeartbeatTime(), MILLISECONDS);
    }

    void heartBeatJob() {
        lock.lock();
        for (RemoteNode node : others) {
            executorService.submit(() -> node.appendEntries(buildPingEntry()));
        }
        startHeartbeat();
        lock.unlock();
    }

    private AppendEntriesRequest buildPingEntry() {
        return AppendEntriesRequest.newBuilder().setTerm(term)
                .setServerId(serverInfo.getServerId())
                .build();
    }

    VoteRequest buildVoteRest() {
        return VoteRequest.newBuilder().setServerId(serverInfo.getServerId())
                .setTerm(term)
                .setLastLogTerm(raftLog.lastLogTerm())
                .setLastLogIndex(raftLog.lastLogTerm())
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
        return clusterConfig.getServersList().contains(serverInfo);
    }

    public boolean inCluster(int server_id) {
        for (Server server : clusterConfig.getServersList()) {
            if (server.getServerId() == server_id) {
                return true;
            }
        }
        return false;
    }

    public Lock getLock() {
        return lock;
    }

    @Override
    public String toString() {
        return "RaftNode{localServer=" + serverInfo.getServerId() + ", state=" + state + ", term=" + term + ", voteFor=" + voteFor + '}';
    }

    public boolean append(byte[] data) {
        try {
            if( leaderId != serverInfo.getServerId()) {
                Logger.info("Server {} is not leader", format(serverInfo));
                return false;
            }
            LogEntry entry = raftLog.appendLog(LogEntry.newBuilder().setTerm(term)
                    .setData(ByteString.copyFrom(data))
                    .setType(0));

            for(RemoteNode remoteNode : others) {
                //CompletableFuture.supplyAsync(remoteNode.appendEntries())
            }

        } catch (Exception e) {

        } finally {

        }
        return false;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }
}
