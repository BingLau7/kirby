package kafkalog;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

/**
 * @author bingjian.lbj
 * @date 2022/2/9-7:14 PM
 **/
public class KafkaFileChannel implements Iterator<ByteBuffer>, Closeable {
    private final String fileName;

    private RandomAccessFile file;
    FileChannel fc;
    private int currentIdx;
    private boolean hasNext;

    public KafkaFileChannel(String fileName) {
        this.fileName = fileName;
        this.currentIdx = 0;
        this.hasNext = true;
    }

    public void init() {
        try {
            this.file = new RandomAccessFile(fileName, "r");
            this.fc = this.file.getChannel();
        } catch (Exception e) {
            throw new RuntimeException("Failed to init " + fileName, e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return hasNext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read", e);
        }
    }

    @Override
    public ByteBuffer next() {
        try {
            this.fc.position(currentIdx);
            ByteBuffer firstBuf = ByteBuffer.allocate(8 + 4);
            this.fc.read(firstBuf);
            firstBuf.position(8);
            int length = firstBuf.getInt();
            if (length == 0) {
                hasNext = false;
                return null;
            }
            ByteBuffer content = ByteBuffer.allocate(length + 12);
            content.position(12);
            this.fc.read(content);
            merge(firstBuf, content);
            this.currentIdx = currentIdx + length + 12;
            return content;
        } catch (EOFException e) {
            try {
                hasNext = false;
                return null;
            } catch (Exception ex) {
                throw new RuntimeException("Failed to close", ex);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read", e);
        }
    }

    private void merge(ByteBuffer firstBuf, ByteBuffer content) {
        content.position(0);
        firstBuf.position(0);
        content.put(firstBuf);
        content.position(0);
    }

    @Override
    public void close() throws IOException {
        if (fc != null) fc.close();
        if (file != null) file.close();
    }
}
