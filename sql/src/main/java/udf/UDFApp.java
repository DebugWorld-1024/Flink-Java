package udf;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;


public class UDFApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        env.setParallelism(1);

        DataStreamSource<String> source = env.fromElements("1", "2", "3", "4");
        tableEnv.createTemporaryView("access", source, $("s"));

        tableEnv.createTemporaryFunction("random_string", new RandomString());

        Table resultTable = tableEnv.sqlQuery("select s, random_string(s) from access");
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute("UDFApp");
    }
}
