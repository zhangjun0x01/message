package test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glodon.pojo.Params;

import java.io.IOException;

public class Mytest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        String s = "{\n" +
                "    \"paimonInfo\":{\n" +
                "        \"warehouse\":\"111\",\n" +
                "        \"uri\":\"111\"\n" +
                "    },\n" +
                "    \"kafkaInfo\":{\n" +
                "        \"bootstrapServer\":\"localhost:9092\",\n" +
                "        \"topic\":\"mytopic\"\n" +
                "    },\n" +
                "    \"rocketmqInfo\":null,\n" +
                "    \"rules\":[\n" +
                "        {\n" +
                "            \"databaseName\":\"db1\",\n" +
                "            \"tableName\":\"mytable\",\n" +
                "            \"monitorType\":\"INSERT\",\n" +
                "            \"selectFields\":\"f1,f2\",\n" +
                "            \"monitorFields\":[\n" +
                "                \"f1\",\n" +
                "                \"f2\",\n" +
                "                \"f3\"\n" +
                "            ],\n" +
                "            \"messageFields\":[\n" +
                "                \"f1\"\n" +
                "            ],\n" +
                "            \"sink\":\"KAFKA\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        Params params = objectMapper.readValue(s, Params.class);
        System.out.println(params);
    }
}
