package io.four.raft.core;

import io.four.raft.proto.Raft;

public class Node {
   public enum NodeState {
        STATE_FOLLOWER,
        STATE_PRE_CANDIDATE,
        STATE_CANDIDATE,
        STATE_LEADER
    }
    protected NodeState state = NodeState.STATE_FOLLOWER;
    protected long term;
    protected Raft.Server serverInfo; // server info
    protected int voteFor;

    public Raft.Server getServerInfo() {
        return serverInfo;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public int getVoteFor() {
        return voteFor;
    }

    public void setVoteFor(int voteFor) {
        this.voteFor = voteFor;
    }

    public NodeState getState() {
        return state;
    }
}

