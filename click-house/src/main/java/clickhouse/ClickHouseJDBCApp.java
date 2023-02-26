package clickhouse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class ClickHouseJDBCApp {
    public static void main(String[] args) throws Exception {

//        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        String url = "jdbc:clickhouse://127.0.0.1:8123/default";
        Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();

        String table_name = "user";
        statement.execute("DROP TABLE IF EXISTS " + table_name);
        statement.execute("CREATE TABLE " + table_name + " (id UInt8 ,name String) ENGINE = Log");
        statement.execute("insert into " + table_name + " (*) values (1, '张三'), (2, '李四'), (3, '王五')");

        ResultSet resultSet = statement.executeQuery("select * from " + table_name);
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            System.out.println(id + "==>" + name);
        }

        resultSet.close();
        statement.close();
        connection.close();
    }
}
