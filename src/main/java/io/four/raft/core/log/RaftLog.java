package io.four.raft.core.log;

import io.four.raft.proto.Raft;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

public class RaftLog {

    static final String META = "raft.meta";
    static final String DATA_SUFFIX = ".log";
    String dir;
    MetaData metaData;
    TreeMap<Long, SegmentLog> logMap;
    long maxSize;

    public RaftLog(String dir, long maxSize) throws Exception {
        this.dir = dir;
        logMap = new TreeMap<>();
        this.maxSize = maxSize;
        File dirFile = new File(dir);
        if (!dirFile.exists()) {
            dirFile.mkdir();
        }
        File[] files = dirFile.listFiles();
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                if (fileName.endsWith(DATA_SUFFIX)) {
                    String strings = fileName.substring(0,fileName.length()-DATA_SUFFIX.length());
                    SegmentLog segmentLog = new SegmentLog(fileName, Long.parseLong(strings));
                    logMap.put(segmentLog.startIndex, segmentLog);
                } else if (fileName.equals(META)) {
                    metaData = new MetaData(fileName);
                }
            }
        }
        if (metaData == null) {
            metaData = new MetaData(metaFileName());
        }
    }

    public long appendLog(Raft.LogEntry entry) throws Exception {
        long index = metaData.getCommitIndex();
        index++;
        metaData.setCommitIndex(index);
        Map.Entry<Long, SegmentLog> mapEntry = logMap.lastEntry();
        SegmentLog segmentLog;
        if (mapEntry == null || mapEntry.getValue() == null || mapEntry.getValue().fileSize() > maxSize) {
            segmentLog = new SegmentLog(dataFileName(index), index);
        } else {
            segmentLog = mapEntry.getValue();
        }
        segmentLog.appendLong(entry);
        return index;
    }

    private final String dataFileName(long index) {
        return dir + File.separator + index + DATA_SUFFIX;
    }

    private final String metaFileName() {
        return dir + File.separator + META;
    }

    public long lastLogIndex() {
        return metaData.getCommitIndex();
    }

    public static void main(String[] args) {
        File file = new File("./data");
        String file1 =  file.getAbsolutePath();
        System.out.println(file1);

    }

}
