package kafkalog;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author bingjian.lbj
 * @date 2022/2/9-7:27 PM
 **/
public class RecordHandlers {
    public final List<RecordHandler> handlers;

    private RecordHandlers() {
        this.handlers = new ArrayList<>();
    }

    public static class InstanceHolder {
        private final static RecordHandlers INSTANCE = new RecordHandlers();
    }

    public static RecordHandlers getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public void addHandler(RecordHandler handler) {
        this.handlers.add(handler);
    }

    public void run(String kafkaPropertiesFile, String topic) {
        List<String> kafkaTopicFiles = getKafkaTopicFiles(kafkaPropertiesFile, topic);
        Executor parseExecutor = Executors.newSingleThreadExecutor();
        List<RecordParser> parsers = new ArrayList<>();
        for (String file : kafkaTopicFiles) {
            parsers.add(new RecordParser(4096, file));
        }
        parsers.forEach(parseExecutor::execute);
        parsers.forEach(p -> {
            while (!p.finished()) {
                try {
                    ConsumerRecord<String, String> record = p.poll(10);
                    if (record != null) {
                        List<ConsumerRecord<String, String>> records = new ArrayList<>(128);
                        records.add(record);
                        records.addAll(p.pollRecords(100));
                        for (ConsumerRecord<String, String> r : records) {
                            handlers.forEach(handler -> handler.handle(r));
                        }
                    }
                } catch (InterruptedException e) {
                    // ignore
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    List<String> getKafkaTopicFiles(String propertiesFile, String topic) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propertiesFile));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load server properties");
        }
        File logDir = new File(properties.getProperty("log.dirs"));
        if (!logDir.exists() || !logDir.isDirectory()) {
            throw new RuntimeException("Failed to get log dir." + properties.getProperty("log.dirs"));
        }
        List<String> partitionDirs = Arrays.stream(logDir.list())
            .filter(f -> {
                String matchStr = topic + "-\\d+";
                return f.matches(matchStr);
            })
            .collect(Collectors.toList());
        if (partitionDirs.isEmpty()) {
            throw new RuntimeException("Failed to get partition dir." + topic);
        }
        return partitionDirs.stream().map(f -> logDir.getAbsolutePath() + "/" + f).collect(Collectors.toList());
    }
}
