package io.four.raft.core;


import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import io.four.raft.proto.Raft;
import org.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Utils {
    static final JsonFormat format = new JsonFormat();

    public static final String format(Message msg) {
       return format.printToString(msg);
    }


    public static int round(int from, int to) {
        return ThreadLocalRandom.current().nextInt(from, to);
    }

    // 127.0.0.1:8081:1
    public static List<Raft.Server> parseServers(String str) {
        String[] arr = str.split(",");
        List<Raft.Server> list = new ArrayList<>();
        for (String s : arr) {
            list.add(parseServer(s));
        }
        return list;
    }

    public static Raft.Server parseServer(String str) {
        String[] arr = str.split(":");
        return Raft.Server.newBuilder().setHost(arr[0])
                .setPort(Integer.parseInt(arr[1]))
                .setServerId(Integer.parseInt(arr[2])).build();
    }

    public static void resetCluster(List<RemoteNode> cluster) {
        for(RemoteNode node : cluster) {
            node.setVoteFor(0);
        }
    }

    public static void main(String[] args) {
        Logger.info("asdada");
    }
}
