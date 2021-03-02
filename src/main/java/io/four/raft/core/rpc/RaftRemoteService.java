package io.four.raft.core.rpc;
import io.four.raft.proto.Raft.*;

public interface RaftRemoteService {

    VoteResponse preVote(VoteRequest request);

    VoteResponse vote(VoteRequest request);
}
