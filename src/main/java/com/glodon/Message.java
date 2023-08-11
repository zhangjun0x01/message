package com.glodon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glodon.filter.DeleteFilter;
import com.glodon.filter.InsertFilter;
import com.glodon.filter.UpdateFilter;
import com.glodon.pojo.PaimonInfo;
import com.glodon.pojo.Params;
import com.glodon.pojo.Rule;
import com.glodon.pojo.Sink;
import com.glodon.sink.RocketmqSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
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


        Params params = objectMapper.readValue(ruleStr, Params.class);

        // build paimon catalog
        String catalogName = "paimon_catalog";
        PaimonInfo connectionParams = params.getPaimonInfo();
        String paimonCatalogSql = String.format("CREATE CATALOG %s WITH (\n"
                + "    'type'='paimon',\n"
                + "    'uri'='%s',\n"
                + "    'metastore'='hive',\n"
                + "    's3.endpoint'='%s',\n"
                + "    's3.access-key'='%s',\n"
                + "    's3.secret-key'='%s',\n"
                + "    'warehouse'='%s'\n"
                + ")", catalogName, connectionParams.getUri(), connectionParams.getS3Endpoint(), connectionParams.getS3AccessKeyId(), connectionParams.getS3SecretAccessKey(), connectionParams.getWarehouse());
        tenv.executeSql(paimonCatalogSql);


        // build kafka sink
        KafkaSinkBuilder<Row> kafkaSinkBuilder = org.apache.flink.connector.kafka.sink.KafkaSink.<Row>builder()
                .setKafkaProducerConfig(new Properties());
        org.apache.flink.connector.kafka.sink.KafkaSink<Row> kafkaSink = kafkaSinkBuilder.build();

        // build rocketmw sink
        RocketmqSink rocketmqSink = new RocketmqSink();

        for (Rule rule : params.getRules()) {
            String sql = String.format("select %s from %s.%s.%s", catalogName, rule.getSelectFields(), rule.getDatabaseName(), rule.getTableName());
            Table table = tenv.sqlQuery(sql);
            DataStream<Row> dataStream = tenv.toChangelogStream(table);

            FilterFunction<Row> function = null;
            switch (rule.getMonitorType()) {
                case INSERT:
                    function = new InsertFilter();
                    break;
                case UPDATE:
                    function = new UpdateFilter(rule.getTableName(), rule.getPrimaryKeys(), rule.getMonitorFields());
                    break;
                case DELETE:
                    function = new DeleteFilter();
                    break;
            }

            DataStream<Row> dataStream1 = dataStream.filter(function);
            Sink sink = rule.getSink();
            if (sink.equals(Sink.KAFKA)) {
                dataStream1.sinkTo(kafkaSink);
            } else if (sink.equals(Sink.ROCKETMQ)) {
                dataStream1.sinkTo(rocketmqSink);
            }
        }
        env.execute();
    }
}
