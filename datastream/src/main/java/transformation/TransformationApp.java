package transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TransformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        transformation(env);
        env.execute("TransformationApp");
    }

    public static void transformation(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.readTextFile("data/input/access.log");
        SingleOutputStreamOperator<Access> map = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] strings = value.split(",");
                Long time = Long.parseLong(strings[0].trim());
                String domain = strings[1].trim();
                Double traffic = Double.parseDouble(strings[2].trim());

                // TODO dict json类型
                return new Access(time, domain, traffic);
            }
        });
        map.map(new EnvRichMapFunction());          // RichFunction实现
//        map.print("map");

        SingleOutputStreamOperator<Access> filter = map.filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access value) throws Exception {
                return value.getTraffic() > 0;
            }
        });

//        filter.print("filter");

        // TODO 相同的key一定在一个task。但是也有可能多个key是被分在同一个分区的
        KeyedStream<Access, String> keyBy = filter.keyBy(new KeySelector<Access, String>() {
            @Override
            public String getKey(Access value) throws Exception {
                return value.getDomain();
            }
        });

//        keyBy.print();
//        keyBy.sum("traffic").print();
        SingleOutputStreamOperator<Access> reduce = keyBy.reduce(new ReduceFunction<Access>() {
            @Override
            public Access reduce(Access value1, Access value2) throws Exception {
                value2.setTraffic(value1.getTraffic() + value2.getTraffic());
                return value2;
            }
        });
        reduce.print();
//        filter.keyBy(Access::getDomain).sum("traffic").print();

    }
}
