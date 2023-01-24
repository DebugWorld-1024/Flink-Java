package state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;


public class CheckpointApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启Checkpoint, TODO 5s之内丢失的呢
         env.enableCheckpointing(5000);
        // 设置模式为精确一次 (这是默认值)
         env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 自定重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,                          // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS)        // 延时
        ));
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.contains("A")){
                    throw new RuntimeException("异常触发");
                }else{
                    return value;
                }
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words){
                    out.collect(word.toLowerCase().trim());
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(x -> x.f0).sum(1).print().setParallelism(1);

        env.execute("CheckpointApp");
    }
}

