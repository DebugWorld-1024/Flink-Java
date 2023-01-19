package source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import utils.MySQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class MysqlSource extends RichSourceFunction<MysqlData> {
    Connection connection;
    PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        assert connection != null;
        statement = connection.prepareStatement("SELECT * FROM exchange_info");
    }

    @Override
    public void close() throws Exception {
        MySQLUtils.close(connection, statement);
    }

    @Override
    public void run(SourceContext<MysqlData> ctx) throws Exception {
        ResultSet rs = statement.executeQuery();
        while (rs.next()){
            int id = rs.getInt("id");
            String exchange_name = rs.getString("exchange_name");
            String exchange_url = rs.getString("exchange_url");
            ctx.collect(new MysqlData(id, exchange_name, exchange_url));
        }
    }

    @Override
    public void cancel() {

    }
}
