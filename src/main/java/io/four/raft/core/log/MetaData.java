package io.four.raft.core.log;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.four.raft.core.Utils.createFile;

public class MetaData {

    private static final int first_off = 0;
    private static final int commit_off = 8;
    private static final int term_off = 16;

    long first_index;
    long commit_index;
    MappedByteBuffer buffer;

    public MetaData(String fileName) throws Exception {
        buffer = new RandomAccessFile(createFile(fileName), "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 24);
        first_index = 1;
        buffer.putLong(first_off, first_index);
        commit_index = buffer.getLong(commit_off);
    }

    public long getFirstIndex() {
        if (first_index != 0) {
            return first_index;
        }
        return buffer.getLong(first_off);
    }

    public long getCommitIndex() {
        if (commit_index != 0) {
            return commit_index;
        }
        return buffer.getLong(commit_off);
    }

    public void setFirstIndex(long index) {
        this.first_index = index;
        buffer.putLong(first_off, index);
    }

    public void setCommitIndex(long index) {
        this.commit_index = index;
        buffer.putLong(commit_off, index);
    }

    public void setTerm(long term) {
        buffer.putLong(term_off, term);
    }

    public long getTerm() {
       return buffer.getLong(term_off);
    }

    @Override
    public String toString() {
        return "{fs_idx=" + first_index + ", ct_index=" + commit_index + ", term=" + getTerm() + '}';
    }
}
