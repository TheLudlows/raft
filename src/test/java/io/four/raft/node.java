package io.four.raft;

import io.four.raft.core.RaftNode;

public class node {
    public static void main(String[] args) throws InterruptedException {
        String servers = args[0];
        String local = args[1];
        RaftNode raftNode = new RaftNode(servers, local, data -> System.out.println("apply data"));
        raftNode.init();

        int n = 1;
        while (true) {
            Thread.sleep(5000);
            if (raftNode.leader()) {
                raftNode.append((n + "").getBytes());
            }
            n++;
        }
    }
}
