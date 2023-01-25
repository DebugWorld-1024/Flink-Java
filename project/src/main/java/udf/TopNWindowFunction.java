package udf;

import domain.EventCategoryProductCount;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TopNWindowFunction implements WindowFunction<Long, EventCategoryProductCount, Tuple3<String, String, String>, TimeWindow> {
    @Override
    public void apply(Tuple3<String, String, String> value, TimeWindow window, Iterable<Long> input, Collector<EventCategoryProductCount> out) throws Exception {

        String event = value.f0;
        String category = value.f1;
        String product = value.f2;
        Long count = input.iterator().next();
        long start = window.getStart();
        long end = window.getEnd();

        out.collect(new EventCategoryProductCount(event, category, product,count,start, end));
    }
}
