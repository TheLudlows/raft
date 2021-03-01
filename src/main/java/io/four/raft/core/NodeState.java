package io.four.raft.core;

public enum NodeState {
    STATE_FOLLOWER,
    STATE_PRE_CANDIDATE,
    STATE_CANDIDATE,
    STATE_LEADER
}
