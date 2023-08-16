package com.glodon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glodon.filter.UpdateFilter;
import com.glodon.pojo.KafkaInfo;
import com.glodon.pojo.PaimonInfo;
import com.glodon.pojo.Params;
import com.glodon.pojo.RocketmqInfo;
import com.glodon.pojo.Rule;
import com.glodon.pojo.Sink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.rocketmq.sink.RocketMQSink;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.table.FileStoreTable;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Message {

    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static void main(String[] args) throws Exception {

//        Preconditions.checkArgument(args.length > 0, "the length of input params must > 0.");
        String ruleStr = "{\n" +
                "    \"paimonInfo\":{\n" +
                "        \"warehouse\":\"111\",\n" +
                "        \"uri\":\"111\"\n" +
                "    },\n" +
                "    \"kafkaInfo\":{\n" +
                "        \"bootstrapServer\":\"localhost:9092\",\n" +
                "        \"topic\":\"mytopic\"\n" +
                "    },\n" +
                "    \"rocketmqInfo\":{\n" +
                "        \"host\":\"localhost:9876\",\n" +
                "        \"topic\":\"tx-mq-TOPIC\"\n" +
                "    },\n" +
                "    \"rules\":[\n" +
                "        {\n" +
                "            \"databaseName\":\"db1\",\n" +
                "            \"tableName\":\"t1\",\n" +
                "            \"selectFields\":\"id,name,age\",\n" +
                "            \"monitorFields\":[\n" +
                "                \"name\"\n" +
                "            ],\n" +
                "            \"messageFields\":[\n" +
                "                \"id\"\n" +
                "            ],\n" +
                "            \"rule\":\"rule1\",\n" +
                "            \"rocketTag\":\"tag1\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"databaseName\":\"db1\",\n" +
                "            \"tableName\":\"t2\",\n" +
                "            \"selectFields\":\"id,classname\",\n" +
                "            \"monitorFields\":[\n" +
                "                \"classname\"\n" +
                "            ],\n" +
                "            \"messageFields\":[\n" +
                "                \"id\"\n" +
                "            ],\n" +
                "            \"rule\":\"rule2\",\n" +
                "            \"rocketTag\":\"tag2\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        Params params = objectMapper.readValue(ruleStr, Params.class);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // build paimon catalog
        String catalogName = "paimon_catalog";
        PaimonInfo connectionParams = params.getPaimonInfo();

        tenv.executeSql(
                "CREATE CATALOG paimon_catalog WITH (\n"
                        + "    'type'='paimon',\n"
                        + "    'warehouse'='file:/Users/zhangjun/work/data/fts'\n"
                        + ")");


//        String paimonCatalogSql = String.format("CREATE CATALOG %s WITH (\n"
//                + "    'type'='paimon',\n"
//                + "    'uri'='%s',\n"
//                + "    'metastore'='hive',\n"
//                + "    's3.endpoint'='%s',\n"
//                + "    's3.access-key'='%s',\n"
//                + "    's3.secret-key'='%s',\n"
//                + "    'warehouse'='%s'\n"
//                + ")", catalogName, connectionParams.getUri(), connectionParams.getS3Endpoint(), connectionParams.getS3AccessKeyId(), connectionParams.getS3SecretAccessKey(), connectionParams.getWarehouse());
//        tenv.executeSql(paimonCatalogSql);


        // build kafka sink
        KafkaSink<String> kafkaSink = buildKafkaSink(params.getKafkaInfo());
        // build rocketmq sink
        RocketMQSink<Tuple2<Row, Rule>> rmqSink = buildRocketMQSink(params.getRocketmqInfo());


//        Rule[] rules = mergeRule(params.getRules());

        for (Rule rule : params.getRules()) {
            boolean res = check(tenv, catalogName, rule.getSelectFields(), rule.getDatabaseName(), rule.getTableName());
            if (!res) {
                continue;
            }
            String sql = String.format("select %s from  %s.%s.%s ", rule.getSelectFields(), catalogName, rule.getDatabaseName(), rule.getTableName());
            Table table = tenv.sqlQuery(sql);
            DataStream<Row> dataStream = tenv.toChangelogStream(table);

            KeyedStream<Tuple2<Row, Rule>, String> keyedStream = dataStream.
                    map(new MapFunction<Row, Tuple2<Row, Rule>>() {
                        @Override
                        public Tuple2<Row, Rule> map(Row row) throws Exception {
                            return Tuple2.of(row, rule);
                        }
                    }).
                    keyBy((KeySelector<Tuple2<Row, Rule>, String>) tuple2 -> tuple2.f1.getTableName());


            FilterFunction<Tuple2<Row, Rule>> function = new UpdateFilter(rule.getTableName(), rule.getMonitorFields());
            DataStream<Tuple2<Row, Rule>> dataStream1 = keyedStream.filter(function);


            Sink sink = rule.getSink();
            if (sink.equals(Sink.KAFKA)) {
//                dataStream1.map(new ResultDealMap(rule)).sinkTo(kafkaSink);
            } else if (sink.equals(Sink.ROCKETMQ)) {
                dataStream1.sinkTo(rmqSink);
            }
        }
        env.execute();
    }

    private static KafkaSink<String> buildKafkaSink(KafkaInfo kafkaInfo) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(kafkaInfo.getBootstrapServer())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaInfo.getTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
    }

    private static RocketMQSink<Tuple2<Row, Rule>> buildRocketMQSink(RocketmqInfo rocketmqInfo) {
        return RocketMQSink.<Tuple2<Row, Rule>>builder()
                .setEndpoints(rocketmqInfo.getHost())
                .setSerializer(new RMQSerializationSchema(rocketmqInfo.getTopic()))
                // If you use transaction message, need set transaction timeout
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setConfig(
                        RocketMQSinkOptions.TRANSACTION_TIMEOUT,
                        TimeUnit.SECONDS.toSeconds(30))
                .build();
    }

//    private static Rule[] mergeRule(Rule[] rules) {
//        Map<String, List<Rule>> result = Arrays.stream(rules).collect(Collectors.groupingBy(m -> m.getDatabaseName() + "_" + m.getTableName()));
//        result.entrySet().stream().map(new Function<Map.Entry<String, List<Rule>>, Object>() {
//            @Override
//            public Object apply(Map.Entry<String, List<Rule>> entry) {
//
//                return null;
//            }
//        })
//
//    }


    //检查数据库、表、字段是否存在
    private static boolean check(StreamTableEnvironment tenv, String catalogName, String selectFields, String databaseName, String tableName) {
        FlinkCatalog catalog = (FlinkCatalog) tenv.getCatalog(catalogName).get();
        Catalog paimonCatalog = catalog.catalog();
        if (!paimonCatalog.databaseExists(databaseName)) {
            return false;
        }

        Identifier identifier = Identifier.create(databaseName, tableName);
        if (!paimonCatalog.tableExists(Identifier.create(databaseName, tableName))) {
            return false;
        }

        try {
            FileStoreTable fileStoreTable = (FileStoreTable) paimonCatalog.getTable(identifier);
            List<String> fields = fileStoreTable.schema().fieldNames();

            String[] selectFieldNames = selectFields.split(",");
            return Arrays.stream(selectFieldNames).allMatch(fields::contains);

        } catch (Catalog.TableNotExistException e) {

        }

        return true;
    }
}
