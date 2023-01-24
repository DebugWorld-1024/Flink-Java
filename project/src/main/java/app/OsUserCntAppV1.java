package app;
import domain.Access;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Objects;


public class OsUserCntAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Access> clearStream = env.readTextFile("data/input/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        // 注意事项：一定要考虑解析的容错性
                        try {
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {
                            e.printStackTrace();   // TODO  处理异常数据到某个地方
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull)
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return "startup".equals(value.event);
                    }
                });
        clearStream.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                return new Tuple3<>(value.os, value.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1);
            }
        }).sum(2).print().setParallelism(1);

        // (iOS,1,40)
        // (Android,1,27)
        env.execute("OsUserCntAppV1");
    }
}
