package io.four.raft.core.log;

import io.four.raft.proto.Raft.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static io.four.raft.core.Utils.toInt;


public class SegmentLog {
    long startIndex;
    long endIndex;
    long size;
    List<LogEntry> logs;
    List<Integer> offs;
    RandomAccessFile file;
    String fileName;

    public SegmentLog (String fileName) throws FileNotFoundException {
        this.fileName = fileName;
        logs = new ArrayList<>();
        offs = new ArrayList<>();
        this.file = new RandomAccessFile(fileName,"RW");
    }

    private void loadData() throws IOException {
        long fileSize = file.length();
        int start = 0;
        while (start < fileSize) {
            LogEntry entry = readEntry(start);
        }
    }

    private LogEntry readEntry(int off) throws IOException {
        byte[] bytes = new byte[4];
        file.read(bytes,off , bytes.length);
        int len = toInt(bytes);
        byte[] data = new byte[len];
        file.read(data, off + 4, len);
        return LogEntry.parseFrom(data);
    }




}
