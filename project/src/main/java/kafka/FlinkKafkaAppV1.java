package kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class FlinkKafkaAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.229.134:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "false");      // 关闭自动保存state
        properties.setProperty("auto.offset.reset","earliest");     // 从最早数据开始接收
        String topic = "kafka_source_topic";
        // kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic kafka_source_topic
        // kafka-console-producer.sh --broker-list master:9092 --topic kafka_source_topic
        // kafka-console-consumer.sh --bootstrap-server master:9092 --topic kafka_result_topic

        // checkpoint 相关参数
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("file:////Users/debugworld/Documents/HelloWorld/JavaProject/Flink-Java/data/checkpoints"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
//        stream.print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                try {
                    String[] splits = value.split(" ");
                    for (String split : splits) {
                        out.collect(Tuple2.of(split.trim(), 1));
                    }
                } catch (Exception e) {
                    // TODO 处理异常数据
                    e.printStackTrace();
                    out.collect(null);
                }

            }
        })
        .filter(Objects::nonNull)
        .keyBy(x ->x.f0)
        .sum(1);
        result.print("kafka source").setParallelism(1);

        env.execute("FlinkKafkaAppV1");
    }
}
