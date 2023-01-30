package utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class FlinkProperties {
    public static void main(String[] args) throws IOException {

        ParameterTool tool = ParameterTool.fromPropertiesFile("data/flink.properties");

        String bootstrapServers = tool.getRequired("bootstrap.servers");
        String groupId = tool.get("group.id", "test");

    }
}
