package slink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import utils.MySQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;


public class MySQLSlink extends RichSinkFunction<Tuple2<String, Integer>> {
    Connection connection;
    PreparedStatement insertStatement;
    PreparedStatement updateStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
//        System.out.println("~~~open~~~");
        // TODO 封装MySQl
        connection = MySQLUtils.getConnection();
        assert connection != null;
        insertStatement = connection.prepareStatement("insert into word_count(word, count) values (?,?)");
        updateStatement = connection.prepareStatement("update word_count set count=? where word=?");
    }

    @Override
    public void close() throws Exception {
//        System.out.println("~~~close~~~");
//        MySQLUtils.close(connection, statement);
        if(insertStatement != null) insertStatement.close();
        if(updateStatement != null) updateStatement.close();
        if(connection != null) connection.close();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        // TODO Windows批量存储
        updateStatement.setInt(1, value.f1);
        updateStatement.setString(2, value.f0);
        updateStatement.execute();

        if(updateStatement.getUpdateCount() == 0) {
            insertStatement.setString(1, value.f0);
            insertStatement.setInt(2 , value.f1);
            insertStatement.execute();
        }
    }
}
