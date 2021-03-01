package io.four.raft.core;

import java.util.concurrent.ThreadLocalRandom;

public class Utils {

    public static int round(int from , int to) {
       return ThreadLocalRandom.current().nextInt(from,to);
    }
}
