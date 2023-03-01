import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class DistributedCacheApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 把对应的数据注册到分布式缓存中
        env.registerCachedFile("data/input/word_count.txt","WordCountDistributedCache");
        DataSource<String> source = env.fromElements("Hadoop", "Spark", "Flink", "PySpark");

        source.map(new RichMapFunction<String, String>() {

            final List<String> list = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 在open方法中如何去获取到分布式缓存中的数据
                File file = getRuntimeContext().getDistributedCache().getFile("WordCountDistributedCache");
                List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                for (String line : lines) {
                    list.add(line);
//                    System.out.println("line-->" + line);
                }
                System.out.println("run open method");

            }

            @Override
            public String map(String value) throws Exception {
                System.out.println("run map method");
                return value;
            }
        }).print();
    }
}
