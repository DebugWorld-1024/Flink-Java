package app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


public class TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
//        DataStreamSource<String> source = env.readTextFile("data/input/topN.txt");
        DataStreamSource<String> source = env.addSource(new DataSource());

        source.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {

                String format = "yyyy-MM-dd HH:mm:ss";
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                try {
                    return sdf.parse(element.split(",")[1].trim()).getTime();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }))
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
//        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
//            @Override
//            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
//                String format = "yyyy-MM-dd HH:mm:ss";
//                SimpleDateFormat sdf = new SimpleDateFormat(format);
//                try {
//                    return sdf.parse(element.f1.trim()).getTime();
//                } catch (ParseException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }))
        .keyBy(x -> x.f0)
//        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
        .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                return Tuple3.of(value2.f0, value2.f1, value1.f2 + value2.f2);
            }
        }, new ProcessWindowFunction<Tuple3<String, String, Long>, Tuple5<String, String, Long, Long, Long>, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Tuple3<String, String, Long>, Tuple5<String, String, Long, Long, Long>, String, TimeWindow>.Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple5<String, String, Long, Long, Long>> out) throws Exception {
                Tuple3<String, String, Long> element = elements.iterator().next();
                out.collect(Tuple5.of(element.f0, element.f1, element.f2, context.window().getStart(), context.window().getEnd()));
            }
        })
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
        .process(new ProcessAllWindowFunction<Tuple5<String, String, Long, Long, Long>, List<Tuple4<String, Long, Long, Long>>, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<Tuple5<String, String, Long, Long, Long>, List<Tuple4<String, Long, Long, Long>>, TimeWindow>.Context context, Iterable<Tuple5<String, String, Long, Long, Long>> elements, Collector<List<Tuple4<String, Long, Long, Long>>> out) throws Exception {
                List<Tuple5<String, String, Long, Long, Long>> list = new ArrayList<Tuple5<String, String, Long, Long, Long>>();
                elements.forEach(list::add);

                list.sort((x,y) -> Long.compare(y.f2, x.f2));
                ArrayList<Tuple4<String, Long, Long, Long>> sorted = new ArrayList<>();
                for (int i = 0; i < Math.min(2, list.size()); i++) {
                    Tuple5<String, String, Long, Long, Long> data = list.get(i);
                    sorted.add(Tuple4.of(data.f0, data.f2, data.f3, data.f4));
                }
                out.collect(sorted);
            }
        })
        .print().setParallelism(1);

        env.execute("TopN");
    }
}


class DataSource implements SourceFunction<String> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Calendar.getInstance().getTimeInMillis();
        Random random = new Random();    // 在指定的数据集中随机选取数据
        String[] users = {"A", "A", "A", "B", "B", "C", "D"};
        while (running) {
            String s = users[random.nextInt(users.length)] + "," + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            ctx.collect(s);
            // 隔1秒生成一个点击事件，方便观测
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
