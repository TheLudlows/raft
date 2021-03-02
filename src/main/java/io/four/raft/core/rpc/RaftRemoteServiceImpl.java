package io.four.raft.core.rpc;
import io.four.raft.core.RaftNode;
import io.four.raft.proto.Raft.*;

public class RaftRemoteServiceImpl implements RaftRemoteService {

    public RaftRemoteServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    RaftNode raftNode;
    @Override
    public VoteResponse preVote(VoteRequest request) {
        Server server = raftNode.getLocalServer();
        VoteResponse.Builder builder= VoteResponse.newBuilder()
                .setServerId(server.getServerId())
                .setGranted(false);
        if(raftNode.getTerm() > request.getTerm() || !raftNode.inCluster(request.getServerId())) {
            return builder.build();
        }
        builder.setGranted(true);
        builder.setTerm(raftNode.getTerm());
        return builder.build();
    }

    @Override
    public VoteResponse vote(VoteRequest request) {
        VoteResponse.Builder builder =  VoteResponse.newBuilder();
        if(raftNode.getTerm() > request.getTerm() || !raftNode.inCluster(request.getServerId())) {
            return builder.build();
        }
        if(request.getTerm() > raftNode.getTerm()) {
            raftNode.toFollower();
        }

        if(raftNode.voteFor() == 0) {

        }
        return builder.setTerm(0)
                .setServerId(raftNode.getLocalServer().getServerId())
                .build();
    }
}
