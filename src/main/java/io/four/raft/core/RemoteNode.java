package io.four.raft.core;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import io.four.raft.core.rpc.RaftRemoteService;
import io.four.raft.proto.Raft.*;
import lombok.Data;

@Data
public class RemoteNode {
    private int term;
    private Server server;
    private long nextIndex;
    private long matchIndex;
    private boolean catchUp;
    private RpcClient rpcClient;
    private RaftRemoteService remoteService;
    public RemoteNode(Server server) {
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(
                server.getHost(),
                server.getPort()));
        remoteService = BrpcProxy.getProxy(rpcClient, RaftRemoteService.class);
        catchUp = false;
    }

    public VoteResponse preVote(VoteRequest voteRequest) {
       return remoteService.preVote(voteRequest);
    }

    public VoteResponse vote(VoteRequest voteRequest) {
        return remoteService.vote(voteRequest);
    }
}
