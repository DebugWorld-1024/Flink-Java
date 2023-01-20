package windows;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class ProcessAllWindowFunctionTmp extends ProcessAllWindowFunction<Integer, Integer, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<Integer, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
        System.out.println("----process invoked...----");
        int maxValue = Integer.MIN_VALUE;
        for (Integer element : elements) {
            maxValue = Math.max(element, maxValue);
        }
        out.collect(maxValue);
    }
}
