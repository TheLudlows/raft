package io.four.raft.core.rpc;

import io.four.raft.core.Node;
import io.four.raft.core.RaftNode;
import io.four.raft.proto.Raft.*;
import org.tinylog.Logger;

import static io.four.raft.core.Utils.format;

public class RaftRemoteServiceImpl implements RaftRemoteService {

    public RaftRemoteServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    RaftNode raftNode;

    @Override
    public VoteResponse preVote(VoteRequest request) {
        raftNode.getLock().lock();
        Server server = raftNode.getServerInfo();
        VoteResponse.Builder builder = VoteResponse.newBuilder()
                .setServerId(server.getServerId())
                .setGranted(false)
                .setTerm(raftNode.getTerm());
        try {
            Logger.info("preVote server {} for:{}", server.getServerId(), format(request));

            if (raftNode.getTerm() <= request.getTerm() && raftNode.inCluster(request.getServerId())) {
                builder.setGranted(true);
                builder.setTerm(raftNode.getTerm());
                Logger.info("preVote server {} pre vote for {}", server.getServerId(), format(request));
            }
            return builder.build();

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
        VoteResponse.Builder builder = VoteResponse.newBuilder();
        builder.setGranted(false).setTerm(raftNode.getTerm());
        try {
            Logger.info("vote server {} vote for {}", raftNode.getServerInfo().getServerId(), format(request));

            if (raftNode.getTerm() > request.getTerm() || !raftNode.inCluster(request.getServerId())) {
                return builder.build();
            }
            if (request.getTerm() > raftNode.getTerm() || raftNode.getVoteFor() == 0) {
                raftNode.toFollower(request.getTerm());
                builder.setGranted(true);
                raftNode.setTerm(request.getTerm());
                raftNode.setVoteFor(request.getServerId());

                VoteResponse response = builder.setTerm(raftNode.getTerm())
                        .setServerId(raftNode.getServerInfo().getServerId())
                        .build();
                Logger.info("vote server {} vote for {}", raftNode.toString(), format(request));
                return response;
            }
        } catch (Exception e) {
            Logger.error("RaftRemoteServiceImpl vote err", e);
        } finally {
            raftNode.getLock().unlock();
        }
        return builder.build();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        raftNode.getLock().lock();
        try {
            Logger.info("appendEntries from {},{}", format(request), raftNode.toString());
            if(request.getTerm() > raftNode.getTerm()) {
                // 无条件服从
                raftNode.toFollower(request.getTerm(), request.getServerId());
            }
            if (raftNode.getState() != Node.NodeState.STATE_LEADER && raftNode.getVoteFor() == request.getServerId()) {
                if (request.getEntriesList().size() == 0) {
                    // ping
                    raftNode.startElectionTask();
                } else {
                    // log
                }
            }
            return AppendEntriesResponse.newBuilder().build();
        } finally {
            raftNode.getLock().unlock();
        }
    }
}
