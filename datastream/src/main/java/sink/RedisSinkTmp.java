package sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class RedisSinkTmp implements RedisMapper<Tuple2<String, Integer>> {
//    @Override
//    public RedisCommandDescription getCommandDescription() {
//        return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
//    }
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET, "WORD_COUNT");
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f1 + "";
    }
}