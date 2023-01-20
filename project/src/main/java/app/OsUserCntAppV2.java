package app;

import com.alibaba.fastjson.JSON;
import domain.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;


public class OsUserCntAppV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Access> clearStream = env.readTextFile("data/input/access.json")
                .map(new MapFunction<String, Access>() {
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
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return "startup".equals(value.event);
                    }
                });
        clearStream.map(new MapFunction<Access, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Access value) throws Exception {
                return new Tuple2<>(value.nu, 1);
            }
        }).keyBy(x -> x.f0).sum(1).print().setParallelism(1);

        // (1,67)
        env.execute("OsUserCntAppV2");
    }
}
