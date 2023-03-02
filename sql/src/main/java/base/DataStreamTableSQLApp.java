package base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class DataStreamTableSQLApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("data/input/access.log");

        SingleOutputStreamOperator<Access> stream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());
                return new Access(time, domain, traffic);
            }
        });

//        // DataStream ==> Table
//        Table table = tableEnv.fromDataStream(stream);
//        tableEnv.createTemporaryView("access", table);
//        Table resultTable = tableEnv.sqlQuery("SELECT * FROM access WHERE domain='a'");
//        // Table ==> DataStream
//        tableEnv.toAppendStream(resultTable, Row.class).print("row:");
//        tableEnv.toAppendStream(resultTable, Access.class).print("access:");

//        Table resultTable = table.select("*").where("domain='imooc.com'");
//        Table resultTable = table.select($("domain"),$("traffic"));
//        tableEnv.toAppendStream(resultTable, Row.class).print("row:");

        Table table = tableEnv.fromDataStream(stream);
        tableEnv.createTemporaryView("access", table);
        Table resultTable = tableEnv.sqlQuery("select domain, sum(traffic) as traffics from access group by domain");
        tableEnv.toRetractStream(resultTable, Row.class).print("row:");
//        tableEnv.toRetractStream(resultTable, Row.class).filter(x -> x.f0).print("row:");


        /**
         * toRetractStream
         * 第一个字段boolean类型表示
         * true：最新的数据
         * false：过期的数据
         */

        env.execute("DataStreamTableSQLApp");
    }
}
