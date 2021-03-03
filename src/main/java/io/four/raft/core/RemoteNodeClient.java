package io.four.raft.core;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import io.four.raft.core.rpc.RaftRemoteService;
import lombok.Data;
import io.four.raft.proto.Raft.*;

import java.util.List;

@Data
public class RemoteNodeClient {
    private int term;
    private Server server;
    private long nextIndex;
    private long matchIndex;
    private boolean catchUp;
    private RpcClient rpcClient;
    private RaftRemoteService remoteService;
    private boolean voted;

    public RemoteNodeClient(Server server) {
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

    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return remoteService.appendEntries(request);
    }

    public static void vote(int serverId, List<RemoteNodeClient> nodes, boolean voted) {
        for(RemoteNodeClient node :nodes) {
            if(node.getServer().getServerId() == serverId) {
                node.setVoted(voted);
                return;
            }
        }
    }

    public static int countVote(List<RemoteNodeClient> nodes) {
        int n = 1;
        for(RemoteNodeClient node :nodes)
            if(node.voted) n++;
        return n;
    }

}
