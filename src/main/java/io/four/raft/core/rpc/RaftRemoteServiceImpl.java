package io.four.raft.core.rpc;

import io.four.raft.core.Node;
import io.four.raft.core.RaftNode;
import io.four.raft.core.log.RaftLog;
import io.four.raft.proto.Raft.*;
import org.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.four.raft.core.Utils.format;

public class RaftRemoteServiceImpl implements RaftRemoteService {

    public RaftRemoteServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    RaftNode raftNode;

    @Override
    public VoteResponse preVote(VoteRequest request) {
        raftNode.getLock().lock();
        VoteResponse.Builder builder = VoteResponse.newBuilder()
                .setServerId(raftNode.getServerInfo().getServerId())
                .setGranted(false)
                .setTerm(raftNode.getTerm());
        try {
            if (raftNode.getTerm() <= request.getTerm() && raftNode.inCluster(request.getServerId())
                    && checkLog(request.getLastLogTerm(), request.getLastLogIndex())) {
                builder.setGranted(true);
            }
            Logger.info("Pre vote from {} voted {}", format(request),builder.getGranted());
        } catch (Exception e) {
            Logger.error("RaftRemoteServiceImpl preVote err", e);
        } finally {
            raftNode.getLock().unlock();
        }
        return builder.build();
    }

    @Override
    public VoteResponse vote(VoteRequest request) {
        raftNode.getLock().lock();
        VoteResponse.Builder builder = VoteResponse.newBuilder()
                .setGranted(false).setTerm(raftNode.getTerm())
                .setServerId(raftNode.getServerInfo().getServerId());
        try {
            if (raftNode.getTerm() > request.getTerm() || !raftNode.inCluster(request.getServerId())) {
                return builder.build();
            }
            if (request.getTerm() > raftNode.getTerm() || raftNode.getVoteFor() == 0) {
                if (checkLog(request.getLastLogTerm(), request.getLastLogIndex())) {
                    raftNode.toFollower(request.getTerm());
                    builder.setGranted(true);
                    raftNode.setTerm(request.getTerm());
                    raftNode.setVoteFor(request.getServerId());
                }
                Logger.info("Vote req {} cur {}", format(request), raftNode.toString());
            }
        } catch (Exception e) {
            Logger.error("RaftRemoteServiceImpl vote err", e);
        } finally {
            raftNode.getLock().unlock();
        }
        return builder.build();
    }

    private boolean checkLog(long term, long index) {
        RaftLog raftLog = raftNode.getRaftLog();
        long lastTerm = raftLog.lastLogTerm();
        long lastIndex = raftLog.lastLogIndex();

        if (lastTerm > term) {
            return false;
        } else if (lastTerm < term) {
            return true;
        } else {
            return lastIndex <= index;
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        raftNode.getLock().lock();
        try {
            Logger.info("AppendEntries from {},{}", format(request), raftNode.toString());
            if (request.getTerm() > raftNode.getTerm()) {
                // 无条件服从
                raftNode.toFollower(request.getTerm(), request.getServerId());
                return buildOkRsp();
            }
            if (raftNode.getState() != Node.NodeState.STATE_LEADER && raftNode.getVoteFor() == request.getServerId()) {
                raftNode.startElectionTask();
                if (request.getPrevLogIndex() != raftNode.getRaftLog().lastLogIndex() || raftNode.getRaftLog().logTerm(request.getPrevLogIndex()) != request.getPrevLogTerm()) {
                    return buildErrRsp();
                }
                if (request.getEntriesList().size() != 0) {
                    // log
                    // check pre
                    long start = request.getPrevLogIndex() + 1;
                    List<LogEntry> es = new ArrayList<>();
                    // 跳过相同的日志
                    for (int i = 0; i < request.getEntriesCount(); i++) {
                        if (request.getEntries(i).getTerm() != raftNode.getRaftLog().logTerm(start)) {
                            es = request.getEntriesList().subList(i, request.getEntriesCount());
                            break;
                        }
                        start++;
                    }
                    // 删除与leader不同的后面的日志
                    while (start < raftNode.getRaftLog().lastLogIndex()) {
                        System.out.println("rm last log");
                        raftNode.getRaftLog().rmLogLast();
                    }
                    for (LogEntry e : es) {
                        raftNode.getRaftLog().incAndGet();
                        raftNode.getRaftLog().appendLog(e);
                    }
                }
                raftNode.tryCommitLog(request);
                return buildOkRsp();
            }
        } catch (Exception e) {
            Logger.warn("Append entry err", e);
        } finally {
            raftNode.getLock().unlock();
        }
        return buildErrRsp();
    }

    private AppendEntriesResponse buildErrRsp() {
        return AppendEntriesResponse.newBuilder().setResCode(2)
                .setLastLogIndex(raftNode.getRaftLog().lastLogIndex())
                .setTerm(raftNode.getTerm())
                .build();
    }

    private AppendEntriesResponse buildOkRsp() {
        return AppendEntriesResponse.newBuilder().setResCode(0)
                .setLastLogIndex(raftNode.getRaftLog().lastLogIndex())
                .setTerm(raftNode.getTerm())
                .build();
    }
}
