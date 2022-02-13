package kafkalog;

import lombok.Data;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author bingjian.lbj
 * @date 2022/2/10-4:28 PM
 **/
@Data
public class IKafkaRecordBatch implements Iterator<IKafkaRecordBatch.IRecord> {
    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;
    private static final int CONTROL_FLAG_MASK = 0x20;
    private static final byte DELETE_HORIZON_FLAG_MASK = 0x40;
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    private BatchHeader header;
    private int currentRecordNum;
    private ByteBuffer byteBuffer;

    public IKafkaRecordBatch(ByteBuffer byteBuffer) {
        this.header = new BatchHeader(byteBuffer);
        this.currentRecordNum = header.recordCount;
        this.byteBuffer = byteBuffer;
    }

    @Data
    public static class BatchHeader {
        private long baseOffset;
        private int length;
        private int partitionLeaderEpoch;
        private byte magic;
        private int crc;
        private short attribute;
        private int lastOffsetDelta;
        private long firstTimestamp;
        private long maxTimestamp;
        private long producerId;
        private short producerEpoch;
        private int sequence;
        private int recordCount;

        public BatchHeader(ByteBuffer byteBuffer) {
            this.baseOffset = byteBuffer.getLong();
            this.length = byteBuffer.getInt();
            this.partitionLeaderEpoch = byteBuffer.getInt();
            this.magic = byteBuffer.get();
            this.crc = byteBuffer.getInt();
            this.attribute = byteBuffer.getShort();
            this.lastOffsetDelta = byteBuffer.getInt();
            this.firstTimestamp = byteBuffer.getLong();
            this.maxTimestamp = byteBuffer.getLong();
            this.producerId = byteBuffer.getLong();
            this.producerEpoch = byteBuffer.getShort();
            this.sequence = byteBuffer.getInt();
            this.recordCount = byteBuffer.getInt();
        }

        public TimestampType timestampType() {
            return (attribute & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
        }

        public CompressionType compressionType() {
            return CompressionType.forId(attribute & COMPRESSION_CODEC_MASK);
        }

        public boolean isTransactional() {
            return (attribute & TRANSACTIONAL_FLAG_MASK) > 0;
        }

        private boolean hasDeleteHorizonMs() {
            return (attribute & DELETE_HORIZON_FLAG_MASK) > 0;
        }

        public boolean isControlBatch() {
            return (attribute & CONTROL_FLAG_MASK) > 0;
        }
    }

    @Data
    public static class IRecord {
        private int length;
        private byte attribute;
        private long timestampDelta;
        private int offsetDelta;
        private byte[] key;
        private byte[] value;
        private RHeader[] headers;

        public IRecord(ByteBuffer byteBuffer) {
            this.length = ByteUtils.readVarint(byteBuffer);
            this.attribute = byteBuffer.get();
            this.timestampDelta = ByteUtils.readVarlong(byteBuffer);
            this.offsetDelta = ByteUtils.readVarint(byteBuffer);
            int keyLen = ByteUtils.readVarint(byteBuffer);
            if (keyLen > -1) {
                this.key = new byte[keyLen];
                byteBuffer.get(key);
            }
            int valLen = ByteUtils.readVarint(byteBuffer);
            if (valLen > -1) {
                this.value = new byte[valLen];
                byteBuffer.get(this.value);
            }
            int headersLen = ByteUtils.readVarint(byteBuffer);
            if (headersLen > -1) {
                this.headers = new RHeader[headersLen];
                for (int i = 0; i < headersLen; i++) {
                    this.headers[i] = new RHeader(byteBuffer);
                }
            }
        }
    }

    @Data
    public static class RHeader {
        private byte[] key;
        private byte[] value;

        public RHeader(ByteBuffer byteBuffer) {
            int keyLen = ByteUtils.readVarint(byteBuffer);
            if (keyLen > -1) {
                this.key = new byte[keyLen];
                byteBuffer.get(key);
            }
            int valLen = ByteUtils.readVarint(byteBuffer);
            if (valLen > -1) {
                this.value = new byte[valLen];
                byteBuffer.get(this.value);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return currentRecordNum > 0;
    }

    @Override
    public IRecord next() {
        currentRecordNum--;
        return new IRecord(this.byteBuffer);
    }
}
