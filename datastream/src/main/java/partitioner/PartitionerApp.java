package partitioner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.AccessSourceV2;
import transformation.Access;


public class PartitionerApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Access> source = env.addSource(new AccessSourceV2());

//        DataStream<Access> stream = source.map(x -> x).partitionCustom(new CustomizePartitionerApp(), new KeySelector<Access, String>() {
//            @Override
//            public String getKey(Access value) throws Exception {
//                return value.getDomain();
//            }
//        });
        DataStream<Access> stream = source.map(x -> x).partitionCustom(new CustomizePartitionerApp(), Access::getDomain);

        stream.map(new MapFunction<Access, Tuple2<String, Access>>() {
            @Override
            public Tuple2<String, Access> map(Access value) throws Exception {
//                System.out.println("current thread id is:" + Thread.currentThread().getId()+ ", value is:" + value.getDomain());
                return new Tuple2<>(value.getDomain(), value);
            }
        }).print();

        System.out.println(source.getParallelism());

        env.execute("PartitionerApp");
    }
}
