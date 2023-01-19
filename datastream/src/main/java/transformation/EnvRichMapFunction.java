package transformation;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;


public class EnvRichMapFunction extends RichMapFunction<Access, Access> {

    /**
     * 初始化操作
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("~~~open~~~");
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("~~~close~~~");
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        System.out.println("~~~getRuntimeContext~~~");
        return super.getRuntimeContext();
    }

    @Override
    public Access map(Access value) throws Exception {
        System.out.println("~~~map~~~");
        return value;
    }
}
