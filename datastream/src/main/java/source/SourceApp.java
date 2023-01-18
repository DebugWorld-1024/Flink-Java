package source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import java.util.Properties;


public class SourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        sourceSocketTextStream(env);
        sourceKafka(env);

        env.execute("SourceApp");
    }

    public static void sourceKafka(StreamExecutionEnvironment env){
//        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.229.131:9092");
        properties.setProperty("group.id", "test");

        DataStream<String> stream = env.addSource(
                new FlinkKafkaConsumer<>("wc_source_topic", new SimpleStringSchema(), properties)
        );
        // vim server.properties
        // 添加 host.name=172.16.229.131
        // kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic wc_source_topic
        // kafka-console-producer.sh --broker-list master:9092 --topic wc_source_topic
        // kafka-console-consumer.sh --bootstrap-server master:9092 --topic wc_result_topic
        // System.out.println(stream.getParallelism());
        // stream.print();

        FlinkKafkaProducer<String> resultTopic = new FlinkKafkaProducer<>(
                "wc_result_topic",
                new SimpleStringSchema(),
                properties
        );
        SingleOutputStreamOperator<String> result = stream.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] worlds = value.split(" ");
                        for (String word : worlds) {
                            out.collect(word.toLowerCase().trim());
                        }
                    }
                }).filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return StringUtils.isNotEmpty(value);
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                }).keyBy(x -> x.f0)
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> value) throws Exception {
                        return "(" + value.f0 + " " + value.f1 + ")";
                    }
                });
        result.print();
        result.addSink(resultTopic);
    }

    public static void sourceSocketTextStream(StreamExecutionEnvironment env){
//        StreamExecutionEnvironment.createLocalEnvironment();
//        StreamExecutionEnvironment.createLocalEnvironment(3);
//        StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment.createRemoteEnvironment("", 8080);

        env.setParallelism(2);
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        System.out.println(source.getParallelism());
        SingleOutputStreamOperator<String> filter = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return true;
            }
        });
        System.out.println(filter.getParallelism());
    }
}

// TODO 序列化
