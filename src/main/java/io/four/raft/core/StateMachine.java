package io.four.raft.core;

public interface StateMachine {

    void apply(byte[] data);
}
