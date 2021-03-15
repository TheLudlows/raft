package io.four.raft.core;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import io.four.raft.core.rpc.RaftRemoteService;
import io.four.raft.proto.Raft.*;
import org.tinylog.Logger;

import java.util.List;

public class RemoteNode extends Node {
    private long nextIndex;
    private long matchIndex;
    private RpcClient rpcClient;
    private RaftRemoteService remoteService;

    public RemoteNode(Server server) {
        this.serverInfo = server;
        this.nextIndex = 1;
        this.rpcClient = new RpcClient(new Endpoint(
                server.getHost(),
                server.getPort()));
        remoteService = BrpcProxy.getProxy(rpcClient, RaftRemoteService.class);
    }

    public VoteResponse preVote(VoteRequest voteRequest) {
        try {
            this.voteFor = 0;
            return remoteService.preVote(voteRequest);
        } catch (Exception e) {
            return buildwarn(serverInfo.getServerId());
        }
    }

    public VoteResponse vote(VoteRequest voteRequest) {
        try {
            this.voteFor = 0;
            return remoteService.vote(voteRequest);
        } catch (Exception e) {
            return buildwarn(serverInfo.getServerId());
        }
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        try {
            return remoteService.appendEntries(request);
        } catch (Exception e) {
            //Logger.warn("RemoteNodeClient appendEntries err {} to {}", format(request), format(serverInfo));
            return buildErrApp();
        }
    }

    public static void vote(int serverId, List<RemoteNode> nodes, int local) {
        for (RemoteNode node : nodes) {
            if (node.getServerInfo().getServerId() == serverId) {
                node.setVoteFor(local);
                return;
            }
        }
    }

    public static int countVote(List<RemoteNode> nodes, int local) {
        int n = 1;
        for (RemoteNode node : nodes)
            if (node.voteFor == local) n++;
        return n;
    }

    VoteResponse buildwarn(int id) {
        return VoteResponse.newBuilder().setGranted(false)
                .setTerm(0).setServerId(id).build();
    }

    AppendEntriesResponse buildErrApp() {
        return AppendEntriesResponse.newBuilder().setResCode(1)
                .build();
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public RemoteNode setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
        return this;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public RemoteNode setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
        return this;
    }

    @Override
    public String toString() {
        return "Node{term=" + term + ", server=" + serverInfo.getServerId() + ", nextIndex=" + nextIndex + ", matchIndex=" + matchIndex + '}';
    }
}
