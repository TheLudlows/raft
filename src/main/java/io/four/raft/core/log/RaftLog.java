package io.four.raft.core.log;

import io.four.raft.proto.Raft.*;
import org.tinylog.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RaftLog {

    static final String META = "raft.meta";
    static final String DATA_SUFFIX = ".log";
    String dir;
    MetaData metaData;
    TreeMap<Long, SegmentLog> logMap;
    long maxSize;
    long lastLogIndex;
    long lastLogTerm;

    public RaftLog(String dir, long maxSize) throws Exception {
        logMap = new TreeMap<>();
        this.maxSize = maxSize;
        File dirFile = new File(dir);
        this.dir = dirFile.getCanonicalPath();
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        File[] files = dirFile.listFiles();
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                if (fileName.endsWith(DATA_SUFFIX)) {
                    String startIndex = fileName.substring(0, fileName.length() - DATA_SUFFIX.length());
                    SegmentLog segmentLog = new SegmentLog(dataFileName(startIndex), Long.parseLong(startIndex));
                    logMap.put(segmentLog.startIndex, segmentLog);
                } else if (fileName.equals(META)) {
                    metaData = new MetaData(metaFileName());
                }
            }
        }
        if (metaData == null) {
            metaData = new MetaData(metaFileName());
        }
        if (logMap.size() > 0) {
            SegmentLog log = logMap.lastEntry().getValue();
            LogEntry last = log.logs.get(log.logs.size() - 1);
            lastLogTerm = last.getTerm();
            lastLogIndex = last.getIndex();
        }

        Logger.info("raft log at {}, meta {}, log size {}", dir, metaData, logMap.size());
    }

    public LogEntry appendLog(LogEntry.Builder builder) throws Exception {
        LogEntry entry = builder.setIndex(++lastLogIndex).build();
        lastLogTerm = entry.getTerm();
        Map.Entry<Long, SegmentLog> mapEntry = logMap.lastEntry();
        SegmentLog segmentLog;
        if (mapEntry == null || mapEntry.getValue() == null || mapEntry.getValue().fileSize() > maxSize) {
            segmentLog = new SegmentLog(dataFileName(lastLogIndex), lastLogIndex);
            logMap.put(lastLogIndex, segmentLog);
        } else {
            segmentLog = mapEntry.getValue();
        }
        segmentLog.appendLong(entry);
        return entry;
    }

    public List<LogEntry> packEntries(long from, int n) {
        long max = from + n;
        List<LogEntry> list = new ArrayList<>();
        while (from < lastLogIndex && from < max) {
            LogEntry logEntry = this.logEntry(from++);
            if (logEntry != null) {
                list.add(logEntry);
            }
        }
        return list;
    }

    public LogEntry logEntry(long index) {
        Map.Entry<Long, SegmentLog> entry = logMap.floorEntry(index);
        if (entry != null) {
            SegmentLog seg = entry.getValue();
            return seg.logEntry(index);
        }
        return null;
    }


    private final String dataFileName(Object index) {
        return dir + File.separator + index + DATA_SUFFIX;
    }

    private final String metaFileName() {
        return dir + File.separator + META;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    public long lastLogTerm() {
        return lastLogTerm;
    }

    public long commitIndex() {
       return metaData.getCommitIndex();
    }

    public void commitIndex(long index) {
        metaData.setCommitIndex(index);
    }

    public static void main(String[] args) throws Exception {
        RaftLog raftLog = new RaftLog("/tmp/data", 10000);
        System.out.println(raftLog.metaData);
        for (int i = 1; i < 10000; i++) {
            raftLog.appendLog(LogEntry.newBuilder()
                    .setType(0).setTerm(1));
        }
    }


}
