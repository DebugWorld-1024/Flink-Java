package state;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
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
                        "ValueState", // the state name
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
        // (1,6.0) (2,3.6666666666666665)
        return Tuple2.of(value.f0, currentState.f1 / currentState.f0.doubleValue());
    }
}


class AvgWithMapState extends RichMapFunction <Tuple2<Long, Long>,Tuple2<Long, Double>> {

    private transient MapState<String, Tuple2<Long, Long>> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor<String, Tuple2<Long, Long>> descriptor = new MapStateDescriptor<>(
                "MapState", // the state name
                TypeInformation.of(new TypeHint<String>(){}),
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
        );
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Long> value) throws Exception {

        Tuple2<Long, Long> tuple2 = mapState.get(value.f0.toString());
        if (tuple2 == null){
            tuple2 = Tuple2.of(0L, 0L);
        }

        long count = tuple2.f0 + 1;
        long sum = tuple2.f1 + value.f1;
//        key = UUID.randomUUID().toString();

        mapState.put(value.f0.toString(), Tuple2.of(count, sum));
        // (1,6.0) (2,3.6666666666666665)
        return Tuple2.of(value.f0, sum / Double.parseDouble(String.valueOf(count)));
    }
}


class AvgWithListState extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    private transient ListState<Long> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<Long>("ListState", Long.class);
        listState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Long> value) throws Exception {
        listState.add(value.f1);
        Iterable<Long> currentState = listState.get();

        long count = 0L;
        long sum = 0L;
        for (Long aLong : currentState) {
            sum += aLong;
            count += 1;
        }
        // (1,6.0) (2,3.6666666666666665)
        return Tuple2.of(value.f0, sum / Double.parseDouble(String.valueOf(count)));
    }
}
