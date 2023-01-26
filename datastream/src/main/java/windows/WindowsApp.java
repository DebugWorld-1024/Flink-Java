package windows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Objects;


public class WindowsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        test01(env);
//        test02(env);
//        test03(env);
        test04(env);
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

    public static void test04(StreamExecutionEnvironment env){
        // 数据格式 flink, 2023-01-25 17:00:00
        // 自定义 trigger

        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);

        source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String format = "yyyy-MM-dd HH:mm:ss";
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                try {
                    return sdf.parse(element.split(",")[1].trim()).getTime();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        })
        .map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String value) throws Exception {
                try {
                    String[] splits = value.split(",");
                    return Tuple3.of(splits[0].trim(), splits[1].trim(), 1L);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        })
        .filter(Objects::nonNull)
        .keyBy(x -> x.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .trigger(new Trigger<Tuple3<String, String, Long>, TimeWindow>() {
            @Override
            public TriggerResult onElement(Tuple3<String, String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                // 每个元素被加入窗口时调用
                return TriggerResult.FIRE;
            }

            @Override
            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                // 在注册的 processing-time timer 触发时调用
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                // 在注册的 event-time timer 触发时调用
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

            }
        })
        .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                return Tuple3.of(value2.f0, value2.f1, value1.f2 + value2.f2);
            }
        })
        .print().setParallelism(1);
    }
}
