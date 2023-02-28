import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.ArrayList;

public class SinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++){
            list.add(i);
        }
//        DataSource<Integer> source = env.fromCollection(list);
        DataSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        source.writeAsText("data/output/data.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute("SinkApp");
    }
}
