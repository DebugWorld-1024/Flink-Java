package windows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        test01(env);
        test02(env);
//        test03(env);
        env.execute("WindowsApp");
    }

    public static void test01(StreamExecutionEnvironment env){
        // 12 13 14
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        })
        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .process(new ProcessAllWindowFunctionTmp())
        .print().setParallelism(1);
    }

    public static void test02(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        source.map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        System.out.println("value1 = [" + value1 + "], value2 = [" + value2 + "]");
                        return value1 + value2;
                    }
                })
                .print().setParallelism(1);
    }

    public static void test03(StreamExecutionEnvironment env){
        // 12 13 14
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        source.map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                })
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .sum(0)
                .print().setParallelism(1);
    }
}
