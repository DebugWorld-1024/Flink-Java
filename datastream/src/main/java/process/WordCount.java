package process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits){
                    out.collect(Tuple2.of(split.trim(), 1L));
                }
            }
        })
        .keyBy(x -> x.f0)
        .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
            private ValueState<Long> accState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("sum-state", Long.class);
                accState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                TimerService timerService = ctx.timerService();
                System.out.println("currentProcessingTime：" + timerService.currentProcessingTime() +
                        "，currentWatermark：" + timerService.currentWatermark());
                System.out.println("Key：" + ctx.getCurrentKey() + "，input element：" + ctx.timestamp());

                // 5.2.2 输出元数据信息
                Long accValue = accState.value();
                if(accValue == null){
                    accState.update(value.f1);
                }else {
                    accState.update(accValue + value.f1);
                }
                // 5.2.3 输出 word 统计结果
                out.collect(new Tuple2<>(value.f0, accState.value()));
            }
        }).print().setParallelism(1);
            
        env.execute("WordCount");
    }
}
