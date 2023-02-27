import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;


public class SourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        // 读取csv文件
//        DataSource<Tuple3<String, Integer, String>> source = env.readCsvFile("data/input/people.csv")
//                .fieldDelimiter(";")                // 更改列的分隔符
//                .ignoreFirstLine()                  // 忽略第一行
//                .types(String.class, Integer.class, String.class);
//        source.print();
//
//        env.readTextFile("data/input/word_count.txt").print();            // 读取文本文件
//        env.readTextFile("data/input/word_count.txt.gz").print();         // 读取压缩文件

        // 读取递归文件
        Configuration parameters = new Configuration();                                        // create a configuration object
        parameters.setBoolean("recursive.file.enumeration", true);                             // set the recursive enumeration parameter
        env.readTextFile("data/input/nest").withParameters(parameters).print();         // pass the configuration to the data source

    }
}
