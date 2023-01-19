package source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CustomizeSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<Access> source = env.addSource(new AccessSource());
//        DataStreamSource<Access> source = env.addSource(new AccessSourceV2());

        DataStreamSource<MySQLData> source = env.addSource(new MySQLSource());
        System.out.println(source.getParallelism());
        source.print();

        env.execute("CustomizeSourceApp");
    }
}

