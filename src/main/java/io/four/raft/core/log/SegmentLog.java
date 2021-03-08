package io.four.raft.core.log;

import io.four.raft.proto.Raft.*;
import org.tinylog.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static io.four.raft.core.Utils.createFile;
import static io.four.raft.core.Utils.format;


public class SegmentLog {
    protected long startIndex;
    protected List<LogEntry> logs;
    List<Long> offs;
    RandomAccessFile file;
    String fileName;

    public SegmentLog (String fileName, long startIndex) throws Exception {
        this.fileName = fileName;
        logs = new ArrayList<>();
        offs = new ArrayList<>();
        this.file = new RandomAccessFile(createFile(fileName),"rw");
        this.startIndex = startIndex;
        this.loadData();
    }

    public LogEntry logEntry(Long index) {
        if(index > startIndex + logs.size()) {
            return null;
        }
        return logs.get((int) (index - startIndex));
    }

    private void loadData() throws IOException {
        long fileSize = file.length();
        long start = 0;
        while (start < fileSize) {
            LogEntry entry = readEntry();
            Logger.info("load data {}", format(entry));
            logs.add(entry);
            offs.add(start);
            start = file.getFilePointer();
        }
    }

    private LogEntry readEntry() throws IOException {
        int len = file.readInt();
        byte[] data = new byte[len];
        file.read(data,0,  len);
        return LogEntry.parseFrom(data);
    }

    public void appendLong(LogEntry entry) throws IOException {
        this.logs.add(entry);
        file.writeInt(entry.getSerializedSize());
        file.write(entry.toByteArray());
        offs.add(file.getFilePointer());
    }

    public long fileSize() throws IOException {
        return file.length();
    }


}
