package windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Objects;


public class Test {
    public static void main(String[] args) throws Exception {
        // 数据格式 flink, 2023-01-25 17:00:00
        // TODO test02 不可以实时打印数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        test01(env);
        test02(env);

        env.execute("Test");
    }

    public static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);

        source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
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
        .sum(2)
        .print().setParallelism(1);
    }

    public static void test02(StreamExecutionEnvironment env){
//        env.setParallelism(1);        // TODO Parallelism设置为1可以打印
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);

        source.map(new MapFunction<String, Tuple3<String, String, Long>>() {
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
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                String format = "yyyy-MM-dd HH:mm:ss";
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                try {
                    return sdf.parse(element.f1).getTime();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        })
        .keyBy(x -> x.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .sum(2)
        .print().setParallelism(1);
    }
}
