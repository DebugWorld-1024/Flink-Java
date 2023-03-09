package window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        /**
         * 窗口大小10s，wm允许是0
         *
         * 0000-9999  user1: 205  user2:6
         * 10000-19999 user1:45
         *
         */
        SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> input = env.fromElements( // 时间(event time),用户名,购买的商品,价格
                "1000,user1,Spark,75",
                "2000,user1,Flink,65",
                "2000,user2,Python,3",
                "3000,user1,Java,65",
                "9999,user2,Hbase,3",
                "19999,user1,Hive,45"
        ).map(new MapFunction<String, Tuple4<Long, String, String, Double>>() {
            @Override
            public Tuple4<Long, String, String, Double> map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0]);
                String user = splits[1];
                String book = splits[2];
                Double money = Double.parseDouble(splits[3]);
                return Tuple4.of(time, user, book, money);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Long, String, String, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Long, String, String, Double>>() {
            @Override
            public long extractTimestamp(Tuple4<Long, String, String, Double> element, long recordTimestamp) {
                return element.f0;
            }
        }));

//        Table table = tableEnv.fromDataStream(input, $("time"), $("user_id"), $("book"), $("money"), $("row_time").rowtime());
////        tableEnv.toRetractStream(table, Row.class).print();
//
//        Table resultTable = table.window(Tumble.over(lit(10).seconds()).on($("row_time")).as("win"))
//                .groupBy($("user_id"), $("win"))
//                .select($("user_id"), $("money").sum().as("total"), $("win").start(), $("win").end());
//        tableEnv.toRetractStream(resultTable, Row.class).filter(x -> x.f0).print();

        tableEnv.createTemporaryView("access", input, $("time"), $("user_id"), $("book"), $("money"), $("row_time").rowtime());
        Table resultTable = tableEnv.sqlQuery("select TUMBLE_START(row_time, interval '10' second) as win_start,TUMBLE_END(row_time, interval '10' second) as win_end, user_id, sum(money) from access group by TUMBLE(row_time, interval '10' second), user_id");
        tableEnv.toRetractStream(resultTable, Row.class).filter(x -> x.f0).print();

        env.execute("WindowApp");
    }
}
