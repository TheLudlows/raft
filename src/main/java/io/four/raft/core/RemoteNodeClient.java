package io.four.raft.core;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import io.four.raft.core.rpc.RaftRemoteService;
import lombok.Data;
import io.four.raft.proto.Raft.*;
import org.tinylog.Logger;

import java.util.List;

import static io.four.raft.core.Utils.format;

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
        try {
            this.voted = false;
            return remoteService.preVote(voteRequest);
        }catch (Exception e) {
            Logger.warn("RemoteNodeClient preVote err {}", voteRequest);
            return buildwarn(voteRequest.getServerId());
        }
    }

    public VoteResponse vote(VoteRequest voteRequest) {
        try {
            this.voted = false;
            return remoteService.vote(voteRequest);
        }catch (Exception e) {
            Logger.warn("RemoteNodeClient vote err {} to {}", format(voteRequest), format(server));
            return buildwarn(voteRequest.getServerId());
        }
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        try {
            return remoteService.appendEntries(request);
        }catch (Exception e) {
            Logger.warn("RemoteNodeClient appendEntries err {} to {}", format(request), format(server));
            return buildErrApp();
        }
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

    VoteResponse buildwarn(int id) {
       return VoteResponse.newBuilder().setGranted(false)
                .setTerm(0).setServerId(id).build();
    }

    AppendEntriesResponse buildErrApp() {
      return   AppendEntriesResponse.newBuilder().setResCode(-1)
                .build();
    }

}
