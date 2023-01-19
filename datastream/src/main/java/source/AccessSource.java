package source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import transformation.Access;

import java.util.Random;


public class AccessSource implements SourceFunction<Access> {
    boolean running = true;

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        String[] domains = {"a", "b", "c", "d"};
        Random random = new Random();
        while (true){
            for(int i = 0; i < 1; i++){
                Access access = new Access();
                access.setTime(123456L);
                access.setDomain(domains[random.nextInt(domains.length)]);
                access.setTraffic(random.nextDouble() * 1000);
                ctx.collect(access);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
