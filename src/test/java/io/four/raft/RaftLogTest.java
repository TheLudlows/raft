package io.four.raft;

import com.google.protobuf.ByteString;
import io.four.raft.core.log.RaftLog;
import io.four.raft.proto.Raft;

import java.nio.charset.Charset;

public class RaftLogTest {

    public static void main(String[] args) throws Exception {
        RaftLog raftLog = new RaftLog("/tmp/raftlog",1024);
        for(int i=1;i<1000;i++ ) {
            raftLog.incAndGet();
            raftLog.appendLog(Raft.LogEntry.newBuilder()
                    .setIndex(i)
                    .setData(ByteString.copyFrom(i+"", Charset.defaultCharset()))
                    .build());
        }

        Raft.LogEntry entry = raftLog.logEntry(500);
        System.out.println(entry);
    }
}
