package io.four.raft.core;


import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import io.four.raft.proto.Raft;

import java.io.File;
import java.io.IOException;
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
        for (RemoteNode node : cluster) {
            node.setVoteFor(0);
        }
    }

    public static final int toInt(byte[] bytes) {
        if (bytes.length != 4) {
            throw new UnsupportedOperationException("unSupport len" + bytes.length);
        }
        int res = 0;
        for (int i = 0; i < bytes.length; i++) {
            res += (bytes[i] & 0xff) << ((3 - i) * 8);
        }
        return res;
    }

    public static final byte[] toByte(int v) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((v >>> 24) & 0xFF);
        bytes[1] = (byte) ((v >>> 16) & 0xFF);
        bytes[2] = (byte) ((v >>> 8) & 0xFF);
        bytes[3] = (byte) ((v >>> 0) & 0xFF);
        return bytes;
    }

    public static final File createFile(String name) throws IOException {
        File file = new File(name);
        if(!file.exists()){
            file.createNewFile();
        }
        return file;
    }

    public static void main(String[] args) {
        int n1 = 356;
        int n2 = 10000000;
        System.out.println(Integer.SIZE);
        System.out.println(toInt(toByte(n1)));
        System.out.println(toInt(toByte(n2)));

    }
}
