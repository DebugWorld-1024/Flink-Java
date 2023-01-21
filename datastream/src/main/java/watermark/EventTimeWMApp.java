package watermark;
import org.apache.commons.lang3.text.CompositeFormat;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *
 * 输入数据的格式：单词,次数,时间字段
 */
public class EventTimeWMApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);

        env.execute("EventTimeWMApp");
    }

    public static void test01(StreamExecutionEnvironment env){
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late-data"){};


        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
//        source.assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {
//            @Override
//            public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                return null;
//            }
//        });

        SingleOutputStreamOperator<String> lines = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[2].trim());
            }
        });

        SingleOutputStreamOperator<String> result = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] strings = value.split(",");
                        return Tuple2.of(strings[0].trim(), 1);
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)
                //        .sum(1).print().setParallelism(1);
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("-----reduce invoked----" + value1.f0 + "==>" + (value1.f1 + value2.f1));
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        for (Tuple2<String, Integer> element : elements) {
                            out.collect("[" + dateFormat.format(context.window().getStart()) + "==>" + dateFormat.format(context.window().getEnd()) + "], " + element.f0 + "==>" + element.f1);
                        }
                    }
                });
        result.print().setParallelism(1);
        result.getSideOutput(outputTag).printToErr();       // 侧流输出
    }
}
