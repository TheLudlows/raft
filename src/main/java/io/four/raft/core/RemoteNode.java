package io.four.raft.core;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import io.four.raft.core.rpc.RaftRemoteService;
import lombok.Data;
import io.four.raft.proto.Raft.*;

import java.util.List;

@Data
public class RemoteNode {
    private int term;
    private Server server;
    private long nextIndex;
    private long matchIndex;
    private boolean catchUp;
    private RpcClient rpcClient;
    private RaftRemoteService remoteService;
    private boolean voted;

    public RemoteNode(Server server) {
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(
                server.getHost(),
                server.getPort()));
        remoteService = BrpcProxy.getProxy(rpcClient, RaftRemoteService.class);
        catchUp = false;
    }

    public VoteResponse preVote(VoteRequest voteRequest) {
        this.voted = false;
        return remoteService.preVote(voteRequest);
    }

    public VoteResponse vote(VoteRequest voteRequest) {
        this.voted = false;
        return remoteService.vote(voteRequest);
    }

    public static void vote(int serverId, List<RemoteNode> nodes, boolean voted) {
        for(RemoteNode node :nodes) {
            if(node.getServer().getServerId() == serverId) {
                node.setVoted(voted);
                return;
            }
        }
    }

    public static int countVote(List<RemoteNode> nodes) {
        int n = 1;
        for(RemoteNode node :nodes)
            if(node.voted) n++;
        return n;
    }

}
