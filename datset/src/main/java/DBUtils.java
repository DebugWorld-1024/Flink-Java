import java.util.Random;

public class DBUtils {

    public static String getConnection() {
        return new Random().nextInt(100) + "";
    }

    public static void releaseConnection(String connection) {

    }
}
