package kafkalog;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author bingjian.lbj
 * @date 2022/2/9-7:31 PM
 **/
public interface RecordHandler {
    ConsumerRecord<String, String> handle(ConsumerRecord<String, String> record);
}
