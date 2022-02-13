package kafkalog;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author bingjian.lbj
 * @date 2022/2/9-7:16 PM
 **/
public class RecordParser implements Runnable {
    private final BlockingQueue<ConsumerRecord<String, String>> recordQueue;
    private final List<KafkaFileChannel> kafkaFileChannels;
    private volatile boolean finished;
    private final String topic;
    private final int partition;

    public RecordParser(int cap, String partitionDir) {
        this.recordQueue = new ArrayBlockingQueue<>(cap);
        File dir = new File(partitionDir);
        this.kafkaFileChannels = Arrays.stream(dir.list()).filter(f -> f.endsWith(".log"))
            .sorted()
            .map(f -> new KafkaFileChannel(partitionDir + "/" + f))
            .peek(KafkaFileChannel::init)
            .collect(Collectors.toList());
        this.finished = false;

        String[] parts = partitionDir.split("-");
        this.partition = Integer.parseInt(parts[parts.length - 1]);
        this.topic = Arrays.stream(parts).limit(parts.length - 1).collect(Collectors.joining("-"));
    }

    @Override
    public void run() {
        for (KafkaFileChannel kafkaFileChannel : kafkaFileChannels) {
            for (Iterator<ByteBuffer> iter = kafkaFileChannel; iter.hasNext(); ) {
                List<ConsumerRecord<String, String>> records = parse(iter.next());
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        recordQueue.put(record);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
            try {
                kafkaFileChannel.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close file channel", e);
            }
        }
        this.finished = true;
    }

    private List<ConsumerRecord<String, String>> parse(ByteBuffer recordData) {
        if (recordData == null) {
            return Collections.emptyList();
        }
        IKafkaRecordBatch recordBatch = new IKafkaRecordBatch(recordData);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (;recordBatch.hasNext();) {
            IKafkaRecordBatch.IRecord iRecord = recordBatch.next();
            String key = Optional.ofNullable(iRecord.getKey()).map(String::new).orElse(null);
            String value = Optional.ofNullable(iRecord.getValue()).map(String::new).orElse(null);
            long offset = recordBatch.getHeader().getBaseOffset() + iRecord.getOffsetDelta();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(topic, partition, offset, key, value);
            records.add(record);
        }
        return records;
    }

    public ConsumerRecord<String, String> poll(int timeoutMs) throws InterruptedException {
        return this.recordQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public List<ConsumerRecord<String, String>> pollRecords(int limit) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>(limit);
        this.recordQueue.drainTo(records);
        return records;
    }

    public boolean finished() {
        return finished && recordQueue.isEmpty();
    }
}
