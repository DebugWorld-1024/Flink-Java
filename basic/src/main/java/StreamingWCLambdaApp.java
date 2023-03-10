import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamingWCLambdaApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("127.0.0.1", 9527);

        stringDataStreamSource.flatMap((String s, Collector<String> collector) ->
                {
                    String[] worlds = s.split(" ");
                    for (String world : worlds) {
                        collector.collect(world.toLowerCase().trim());
                    }
                })
                .returns(Types.STRING)              // Lambda表达式使用Java泛型时，就需要声明返回数据的类型。参数的泛型，Java编译器编译该代码时会进行参数类型擦除
                .filter(StringUtils::isNotBlank)
                .returns(Types.STRING)
                .map(s -> s)                        // 返回值s不是泛型，就不需要returns
                .map(s -> new Tuple2<>(s,1))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .sum(1).print();
        /*
        stringDataStreamSource.flatMap((FlatMapFunction<String, String>) (String s, Collector<String> collector) ->
                {
                    String[] worlds = s.split(" ");
                    for (String world : worlds) {
                        collector.collect(world.toLowerCase().trim());
                    }
                })
                .returns(Types.STRING)
                .filter((FilterFunction<String>) StringUtils::isNotBlank)
                .returns(Types.STRING)
                .map((MapFunction<String, Tuple2<String, Integer>>) s -> new Tuple2<>(s,1))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .sum(1).print();
         */

        env.execute("StreamingWCLambdaApp");
    }
}

// TODO Lambda表达式
// TODO stream用法
