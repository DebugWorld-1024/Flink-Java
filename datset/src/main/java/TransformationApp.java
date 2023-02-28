import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class TransformationApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        testMapPartition(env);
//        testDistinct(env);
        testFirstN(env);
    }

    public static void testFirstN(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer,String>> list = new ArrayList<>();

        list.add(Tuple2.of(1, "Hadoop"));
        list.add(Tuple2.of(1, "Spark"));
        list.add(Tuple2.of(1, "Flink"));
        list.add(Tuple2.of(1, "Hadoop"));
        list.add(Tuple2.of(2, "Java"));
        list.add(Tuple2.of(2, "Spring"));
        list.add(Tuple2.of(2, "Linux"));
        list.add(Tuple2.of(2, "Vue"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(list);
        data.groupBy(0).first(2).print();
    }

    public static void testDistinct(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        list.add("hadoop,spark");
        list.add("hadoop,flink");
        list.add("flink,flink");

        DataSource<String> data = env.fromCollection(list);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        }).distinct().print();
    }

    public static void testMapPartition(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add("student:" + i);
        }

        DataSource<String> data = env.fromCollection(list).setParallelism(5);
        System.out.println("====" + data.getParallelism());

        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                String connection = DBUtils.getConnection();
                System.out.println("connection: " + connection);

                // TODO... 业务逻辑处理

                DBUtils.releaseConnection(connection);
            }
        }).print();

        data.map(new MapFunction<String, String>() {

            /**
             * 假设：进来一条数据，要与业务库中的数据进行相关的操作
             * Connection
             */
            @Override
            public String map(String value) throws Exception {

                String connection = DBUtils.getConnection();
                System.out.println("connection: " + connection);

                // TODO... 业务逻辑处理

                DBUtils.releaseConnection(connection);

                return value;
            }
        }); // .print();

    }
}
