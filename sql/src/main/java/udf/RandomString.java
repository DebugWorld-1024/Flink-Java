package udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import java.util.Random;


public class RandomString extends ScalarFunction {
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        System.out.println("UDF open method is running");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("UDF close method is running");
    }

    public String eval(String ip) {
        Random random = new Random();
        String[] strings = {"A", "B", "C", "D", "E", "F", "G"};
        String s = strings[random.nextInt(strings.length)];
//        System.out.println("UDF eval method is running");
        return ip + "-" + s;
    }
}
