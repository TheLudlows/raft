package io.four.raft.core;

import java.util.*;
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
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RaftNode extends Node {
    private RaftConfig config;
    private List<RemoteNode> others;
    private ClusterConfig clusterConfig;
    private StateMachine stateMachine;

    private final Lock lock = new ReentrantLock();

    private CompletableFuture electionFuture;
    private CompletableFuture heartbeatFuture;
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
            this.rpcServer = new RpcServer(serverInfo.getHost(), serverInfo.getPort());
            this.raftLog = new RaftLog(config.getDir() + serverInfo.getServerId(), config.getMaxFileSize());

            this.rpcServer.registerService(stateMachine);
            this.rpcServer.registerService(new RaftRemoteServiceImpl(this));
            rpcServer.start();
            startElectionTask();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public RaftNode(String servers, String serverInfo, StateMachine stateMachine) {
        this(parseServers(servers), parseServer(serverInfo), stateMachine, new RaftConfig());
    }

    public void startElectionTask() {
        if (electionFuture != null && !electionFuture.isDone()) {
            electionFuture.cancel(true);
        }
        long time = round(config.getElectionFrom(), config.getElectionTo());
        electionFuture = CompletableFuture.runAsync(() -> preVote(), delayedExecutor(time, MILLISECONDS));
    }

    void preVote() {
        lock.lock();
        try {
            Logger.info("Start preVote");
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
                        .whenCompleteAsync((r, e) -> processPreVoteResp(r, term));
            }
            startElectionTask();
        } finally {
            lock.unlock();
        }
    }

    private void processPreVoteResp(VoteResponse response, long oldTerm) {
        lock.lock();
        try {
            Logger.info("Pre vote resp {}", format(response));
            // if pass start vote to be candidate
            if (state != STATE_PRE_CANDIDATE || term != oldTerm) {
                Logger.info("Rec old vote resp {} old term {}", response.getServerId(), oldTerm);
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
    }

    private void processVoteResp(VoteResponse response, long oldTerm) {
        lock.lock();
        Logger.info("Rec vote from {}", format(response));
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
            voteFor = serverInfo.getServerId();
            if (electionFuture != null && !electionFuture.isDone()) {
                electionFuture.cancel(true);
            }
            Logger.info("Become leader {}", this.toString());
            startHeartbeat();
        } catch (Exception e) {
            Logger.info("to leader err ", e);
        }
    }

    public void toFollower(long term) {
        state = STATE_FOLLOWER;
        this.term = term;
        Logger.info("To follower {}", this.toString());
        if (heartbeatFuture != null && heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(true);
        }
        startElectionTask();
    }

    public void toFollower(long term, int leaderId) {
        this.voteFor = leaderId;
        toFollower(term);
    }


    private void startHeartbeat() {
        if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(true);
        }
        heartbeatFuture = CompletableFuture.runAsync(() -> heartBeatJob(), delayedExecutor(config.getHeartbeatTime(), MILLISECONDS));
    }

    void heartBeatJob() {
        lock.lock();
        for (RemoteNode node : others) {
            CompletableFuture.supplyAsync(() -> appendLog(node));
        }
        startHeartbeat();
        lock.unlock();
    }

    VoteRequest buildVoteRest() {
        return VoteRequest.newBuilder().setServerId(serverInfo.getServerId())
                .setTerm(term)
                .setLastLogTerm(raftLog.lastLogTerm())
                .setLastLogIndex(raftLog.lastLogIndex())
                .build();
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
        return "RaftNode{term=" + term + ", voteFor=" + voteFor +
                " " + raftLog.toString() + '}';
    }

    public boolean append(byte[] data) {
        boolean res = false;
        try {
            lock.lock();
            System.out.println("start append");
            if (voteFor != serverInfo.getServerId()) {
                Logger.info("Server {} is not leader", format(serverInfo));
                return false;
            }
            raftLog.appendLog(LogEntry.newBuilder().setTerm(term)
                    .setData(ByteString.copyFrom(data))
                    .setType(0)
                    .setIndex(raftLog.incAndGet()).build());
            List<CompletableFuture<Boolean>> list = new ArrayList<>();
            for (RemoteNode remoteNode : others) {
                list.add(CompletableFuture.supplyAsync(() -> appendLog(remoteNode))
                        .orTimeout(config.getAppendTimeOut(), MILLISECONDS)
                        .exceptionally(t -> false));
            }
            lock.unlock();
            for (CompletableFuture<Boolean> future : list) {
                res = res || future.get();
            }
        } catch (Exception e) {
            Logger.error("Append data err", e);
        }
        return res;
    }

    boolean appendLog(RemoteNode node) {
        try {
            lock.lock();
            long from = node.getNextIndex();
            int n = config.getMaxAppendLogs();
            List<LogEntry> logEntries = raftLog.packEntries(from, n);
            AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
            for (LogEntry entry : logEntries) {
                builder.addEntries(entry);
            }
            long lastIndex = node.getNextIndex() - 1;
            long lastTerm = raftLog.logTerm(lastIndex);
            AppendEntriesRequest request = builder.setTerm(term)
                    .setCommitIndex(raftLog.commitIndex())
                    .setPrevLogIndex(lastIndex)
                    .setServerId(serverInfo.getServerId())
                    .setPrevLogTerm(lastTerm)
                    .build();

            AppendEntriesResponse response = node.appendEntries(request);
            Logger.info("Append log req {} to {} response {}, {}", format(request), node.toString(), format(response), raftLog.toString());
            if (response.getResCode() == 1) {
                return false;
            } else if (response.getResCode() == 2) {
                node.setNextIndex(response.getLastLogIndex() + 1);
                return false;
            }

            if (response.getTerm() > term) {
                toFollower(response.getTerm());
                return false;
            }
            node.setNextIndex(response.getLastLogIndex() + 1);
            node.setMatchIndex(from - 1 + logEntries.size());
            return tryCommitLog();// 尝试去提交(应用状态机)
        } catch (Exception e) {
            Logger.error("try commit err", e);
        } finally {
            lock.unlock();
        }
        return false;
    }

    // leader commit
    private boolean tryCommitLog() {
        try {
            int clusterSize = clusterConfig.getServersList().size();
            long commitIndex = raftLog.commitIndex();
            long[] appendIndexArr = new long[clusterSize];
            appendIndexArr[0] = raftLog.lastLogIndex();
            int i = 1;
            for (RemoteNode node : others) {
                appendIndexArr[i++] = node.getMatchIndex();
            }

            Arrays.sort(appendIndexArr);
            long appendIndex = appendIndexArr[clusterSize / 2];

            if (appendIndex > raftLog.lastLogIndex()) {
                Logger.warn("appendIndex big than raftLog.lastLogIndex {} {} {}", appendIndex, raftLog.lastLogIndex(), others);
                return false;
            }
            if (appendIndex <= commitIndex) {
                return false;
            }
            Long start = commitIndex + 1;
            for (; start <= appendIndex; start++) {
                stateMachine.apply(raftLog.logEntry(start).getData().toByteArray());
            }
            Logger.info("Try commit log cur node {}, cluster {}", this.toString(), others.toString());
            raftLog.commitIndex(appendIndex);
            return true;
        } catch (Exception e) {
            Logger.error("append log err", e);
        }
        return false;
    }

    // follower commit
    public void tryCommitLog(AppendEntriesRequest request) {
        long leaderCommit = request.getCommitIndex();
        long commitIndex = raftLog.commitIndex() + 1;
        long lastIndex = raftLog.lastLogIndex();
        while (commitIndex <= leaderCommit && commitIndex <= lastIndex) {
            stateMachine.apply(raftLog.logEntry(commitIndex).getData().toByteArray());
            System.out.println("follow apply " + commitIndex);
            commitIndex++;
        }
        if (leaderCommit > raftLog.commitIndex()) {
            System.out.println("leader  commit " + leaderCommit);
            raftLog.commitIndex(leaderCommit);
        }
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public boolean leader() {
        return state == STATE_LEADER;
    }
}
