package kafkalog;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * @author bingjian.lbj
 * @date 2022/2/10-3:12 PM
 **/
public class KafkaTest {
    @Test
    public void testChannel() {
        KafkaFileChannel channel = new KafkaFileChannel("/private/tmp/kafka-logs/dove_1-0/00000000000000000000.log");
        channel.init();
        for (Iterator<ByteBuffer> iter = channel; iter.hasNext(); ) {
            ByteBuffer bf = iter.next();
            IKafkaRecordBatch recordBatch = new IKafkaRecordBatch(bf);
            for (;recordBatch.hasNext();) {
                IKafkaRecordBatch.IRecord record = recordBatch.next();
                System.out.println("key: " + new String(record.getKey()) + "; value: " + new String(record.getValue()));
            }
        }
    }

    @Test
    public void testGetTopicPartitionDirs() {
        List<String> files = RecordHandlers.getInstance().getKafkaTopicFiles("/Users/bingjian.lbj/work/open-source/kafka/config/server.properties", "dove_1");
        System.out.println(files);
    }

    @Test
    public void runPartition() {
        List<String> files = RecordHandlers.getInstance().getKafkaTopicFiles("/Users/bingjian.lbj/work/open-source/kafka/config/server.properties", "dove_1");
        RecordParser recordParser = new RecordParser(4096, files.get(0));
        // 正常应该是多线程
        recordParser.run();
        System.out.println(recordParser.pollRecords(1000));
    }

    @Test
    public void testRun() {
        RecordHandlers.getInstance().addHandler(record -> {
            System.out.println(record);
            return record;
        });
        RecordHandlers.getInstance().run("/Users/bingjian.lbj/work/open-source/kafka/config/server.properties", "dove_1");
    }
}
