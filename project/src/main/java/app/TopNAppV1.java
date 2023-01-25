package app;
import com.alibaba.fastjson.JSON;
import domain.Access;
import domain.EventCategoryProductCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import udf.TopNAggregateFunction;
import udf.TopNWindowFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class TopNAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("data/input/access.json");
//        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);

        source.print().setParallelism(1);
        SingleOutputStreamOperator<Access> clearStream = source.map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        try {
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(20)) {
                    @Override
                    public long extractTimestamp(Access element) {
                        return element.time;
                    }
                })
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return !"startup".equals(value.event);
                    }
                });

        WindowedStream<Access, Tuple3<String, String, String>, TimeWindow> windowStream = clearStream.keyBy(new KeySelector<Access, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Access value) throws Exception {
                return Tuple3.of(value.event, value.product.category, value.product.name);
            }
        }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)));
        SingleOutputStreamOperator<EventCategoryProductCount> aggStream = windowStream.aggregate(new TopNAggregateFunction(), new TopNWindowFunction());
//        aggStream.print().setParallelism(1);
        aggStream.keyBy(new KeySelector<EventCategoryProductCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(EventCategoryProductCount value) throws Exception {
                return Tuple4.of(value.event, value.category, value.start, value.end);
            }
        }).process(new KeyedProcessFunction<Tuple4<String,String,Long,Long>, EventCategoryProductCount, List<EventCategoryProductCount>>() {

            private transient ListState<EventCategoryProductCount> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<EventCategoryProductCount>("cnt-state", EventCategoryProductCount.class));
            }

            @Override
            public void processElement(EventCategoryProductCount value, Context ctx, Collector<List<EventCategoryProductCount>> out) throws Exception {
                listState.add(value);

                // 注册一个定时器
                ctx.timerService().registerEventTimeTimer(value.end + 1);
            }


            // 在这里完成TopN操作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<EventCategoryProductCount>> out) throws Exception {
                ArrayList<EventCategoryProductCount> list = Lists.newArrayList(listState.get());

                list.sort((x,y) -> Long.compare(y.count, x.count));

                ArrayList<EventCategoryProductCount> sorted = new ArrayList<>();

                for (int i = 0; i < Math.min(3, list.size()); i++) {
                    EventCategoryProductCount bean = list.get(i);
                    sorted.add(bean);
                }

                out.collect(sorted);
            }
        }).print().setParallelism(1);

        env.execute("TopNAppV1");
    }
}
