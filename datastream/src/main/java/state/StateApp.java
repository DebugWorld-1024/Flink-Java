package state;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class StateApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test01(env);

        env.execute("StateApp");
    }

    /**
     * 使用ValueState实现平均数
     * @param env
     */
    public static void test01(StreamExecutionEnvironment env){
        env.setParallelism(1);
        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
        list.add(Tuple2.of(1L, 3L));
        list.add(Tuple2.of(1L, 7L));
        list.add(Tuple2.of(2L, 4L));
        list.add(Tuple2.of(1L,8L));
        list.add(Tuple2.of(2L, 2L));
        list.add(Tuple2.of(2L, 5L));

        DataStreamSource<Tuple2<Long, Long>> source = env.fromCollection(list);
        source.keyBy(x -> x.f0).map(new AvgWithMapState()).print().setParallelism(1);
    }

}

// TODO 为什么用FlatMap
//class AvgWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>>{
//    private transient ValueState<Tuple2<Long, Long>> sum;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
//                new ValueStateDescriptor<>(
//                        "average", // the state name
//                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // default value of the state, if nothing was set
//        sum = getRuntimeContext().getState(descriptor);
//    }
//
//
//    @Override
//    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
//        Tuple2<Long, Long> currentState = sum.value();
//
//        if (currentState == null){
//            currentState = Tuple2.of(0L, 0L);
//        }
//        currentState.f0 += 1;
//        currentState.f1 += value.f1;
//        sum.update(currentState);
//
//        out.collect(Tuple2.of(value.f0, currentState.f1 / currentState.f0.doubleValue()));
//    }
//}

class AvgWithValueState extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    private transient ValueState<Tuple2<Long, Long>> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // default value of the state, if nothing was set
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Long> value) throws Exception {
        Tuple2<Long, Long> currentState = valueState.value();

        if (currentState == null){
            currentState = Tuple2.of(0L, 0L);
        }
        currentState.f0 += 1;
        currentState.f1 += value.f1;
        valueState.update(currentState);

        return Tuple2.of(value.f0, currentState.f1 / currentState.f0.doubleValue());
    }
}


class AvgWithMapState extends RichMapFunction <Tuple2<Long, Long>,Tuple2<Long, Double>> {

    // TODO Tuple2<Long, Long>
    private transient MapState<String, String> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>(
                "average", // the state name
                String.class,
                String.class
        );
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Long> value) throws Exception {

        String s = mapState.get(value.f0.toString());
        if (s == null){
            s = "0,0";
        }
        String[] splits = s.split(",");

        long count = Integer.parseInt(splits[0].trim()) + 1;
        long sum = Long.parseLong(splits[1].trim()) + value.f1;
//        key = UUID.randomUUID().toString();

        mapState.put(value.f0.toString(), count + "," + sum);
        return Tuple2.of(value.f0, sum / Double.parseDouble(String.valueOf(count)));
    }
}