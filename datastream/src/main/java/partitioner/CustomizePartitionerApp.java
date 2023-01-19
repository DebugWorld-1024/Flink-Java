package partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomizePartitionerApp implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
//        System.out.println("numPartitions:" + numPartitions);

        if("a".equals(key)) {
            return 0;
        } else if("b".equals(key)) {
            return 1;
        } else if("c".equals(key)) {
            return 2;
        }else if("d".equals(key)){
            return 3;
        }else {
            return 4;
        }
    }
}
