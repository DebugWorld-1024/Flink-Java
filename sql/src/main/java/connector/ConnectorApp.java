package connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class ConnectorApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new FileSystem().path("data/input/access.log"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("timestamp", DataTypes.BIGINT())
                        .field("domain", DataTypes.STRING())
                        .field("traffic", DataTypes.DOUBLE())
                ).createTemporaryTable("access_ods");

        Table accessOds = tableEnv.from("access_ods");

        Table resultTable = accessOds.groupBy($("domain"))
                .aggregate($("traffic").sum().as("traffics"))
                .select($("domain"), $("traffics"));
        tableEnv.toRetractStream(resultTable, Row.class).print();

        // Csv() 不支持更新操作
        tableEnv.connect(new FileSystem().path("data/output"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("domain", DataTypes.STRING())
                        .field("traffic", DataTypes.DOUBLE())
                ).createTemporaryTable("access_ads");

        resultTable.executeInsert("access_ads");

        env.execute("ConnectorApp");
    }
}
