package sink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;


public class CustomizeSinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> source = env.readTextFile("data/input/word_count.txt");
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.flatMap((String s, Collector<String> collector) ->
                {
                    String[] worlds = s.split(",");
                    for (String world : worlds) {
                        collector.collect(world.toLowerCase().trim());
                    }
                })
                .returns(Types.STRING)              // Lambda表达式使用Java泛型时，就需要声明返回数据的类型。参数的泛型，Java编译器编译该代码时会进行参数类型擦除
                .filter(StringUtils::isNotBlank)
                .returns(Types.STRING)
                .map(s -> s)                        // 返回值s不是泛型，就不需要returns
                .map(s -> new Tuple2<>(s, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .sum(1);

        result.addSink(new MySQLSink());

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPassword("123456").build();
        result.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisSinkTmp()));
        env.execute("CustomizeSinkApp");
    }
}

