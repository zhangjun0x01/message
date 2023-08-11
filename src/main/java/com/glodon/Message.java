package com.glodon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glodon.filter.DeleteFilter;
import com.glodon.filter.InsertFilter;
import com.glodon.filter.UpdateFilter;
import com.glodon.pojo.KafkaSinkPojo;
import com.glodon.pojo.RocketSink;
import com.glodon.pojo.Rule;
import com.glodon.pojo.Sink;
import com.glodon.sink.RocketmqSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class Message {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void main(String[] args) throws Exception {

        String ruleStr = args[0];


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql(
                "CREATE CATALOG fts_catalog WITH (\n"
                        + "    'type'='paimon',\n"
                        + "    'warehouse'='file:/Users/zhangjun/work/data/fts'\n"
                        + ")");

        tenv.executeSql("use catalog fts_catalog");
        Rule[] rules = objectMapper.readValue(ruleStr, Rule[].class);

        KafkaSinkBuilder<Row> kafkaSinkBuilder = KafkaSink.<Row>builder()
                .setKafkaProducerConfig(new Properties());
        KafkaSink<Row> kafkaSink = kafkaSinkBuilder.build();

        RocketmqSink rocketmqSink = new RocketmqSink();

        for (Rule rule : rules) {
            String sql = String.format("select %s from fts_catalog.%s.%s", rule.getSelectFields(), rule.getDatabaseName(), rule.getTableName());
            Table table = tenv.sqlQuery(sql);
            DataStream<Row> dataStream = tenv.toChangelogStream(table);

            FilterFunction<Row> function = null;
            switch (rule.getKind()) {
                case "INSERT":
                    function = new InsertFilter();
                    break;
                case "UPDATE":
                    function = new UpdateFilter(rule.getTableName(), rule.getPrimaryKeys(), rule.getMonitorFields());
                    break;
                case "DELETE":
                    function = new DeleteFilter();
                    break;
            }

            DataStream<Row> dataStream1 = dataStream.filter(function);


            Sink sink = rule.getSink();
            if (sink instanceof KafkaSinkPojo) {
                dataStream1.sinkTo(kafkaSink);
            } else if (sink instanceof RocketSink) {
                dataStream1.sinkTo(rocketmqSink);
            }
        }
        env.execute();
    }
}
