package io.four.raft.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import io.four.raft.proto.Raft;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Utils {
    public static Logger LOG;
    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        //设置全局日志级别
        LOG = loggerContext.getLogger("root");
        LOG.setLevel(Level.toLevel("info"));
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
}
