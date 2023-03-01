import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Flink中计数器/累加器的使用
 */
public class CounterApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.fromElements("Hadoop", "Spark", "Flink", "PySpark");

        /**
         * 要实现一个ETL的功能：
         * 数据清洗：ip=>省份、地市、运营商之类   一个字段拆分成多个字段
         * 数据类型的转换： 流量   关键的字段，必须是一个数值类型，  有可能进来的字符串类型，那么就需要转换成数值类型
         * ...
         */

        MapOperator<String, String> info = source.map(new RichMapFunction<String, String>() {
            final LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("ele-cnts", counter);
            }

            @Override
            public String map(String value) throws Exception {
                /**
                 * todo... 数据清洗功能 & 计数功能
                 *
                 * 很有可能遇到不符合规则的数据
                 */
                counter.add(1);

//                try {
//
//                }catch (){
//                    // 记录错误数据的计数器+1
//                }



                return value;
            }
        });

        info.writeAsText("data/output/CounterApp", FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        JobExecutionResult result = env.execute("CounterApp");

        Object acc = result.getAccumulatorResult("ele-cnts");
        System.out.println(acc);
    }
}
