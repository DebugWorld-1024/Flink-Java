import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWCApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = env.readTextFile("data/input/word_count.txt");

        stringDataSource.flatMap(new EnvFlatMapFunction())
                .map(new EnvMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

//        env.execute("BatchWCApp");
    }
}

class EnvFlatMapFunction implements FlatMapFunction<String, String>{
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] words = value.split(",");
        for (String word: words){
            out.collect(word.toLowerCase().trim());
        }
    }
}

class EnvMapFunction implements MapFunction<String, Tuple2<String, Integer>>{
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<>(value, 1);
    }
}
