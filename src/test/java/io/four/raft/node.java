package io.four.raft;

import io.four.raft.core.RaftNode;

public class node {
    public static void main(String[] args) {
        String servers = args[0];
        String local = args[1];
        new Thread(() -> new RaftNode(servers, local, data -> System.out.println("apply")).init()).start();
    }
}
