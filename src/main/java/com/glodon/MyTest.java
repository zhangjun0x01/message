package com.glodon;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Properties;

public class MyTest {
    public void main(String[] args) throws Exception {
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

        ArrayList<Rule> rules = new ArrayList<>();
        rules.add(new Rule("db1", "t1", "INSERT", new String[]{"id", "name"}, "myrule1"));
        rules.add(new Rule("db1", "t2", "INSERT", new String[]{"id", "name"}, "myrule1"));


        //KAFKA sink
//        final TopicSelector<String> topicSelector =
//                (e) -> {
//                    if (e.equals("a")) {
//                        return "topic-a";
//                    }
//                    return "topic-b";
//                };
//        final KafkaRecordSerializationSchemaBuilder<Row> builder =
//                KafkaRecordSerializationSchema.builder().setTopicSelector(topicSelector);
//        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
//        final KafkaRecordSerializationSchema<String> schema =
//                builder.setValueSerializationSchema(serializationSchema).build();

        KafkaSinkBuilder<Row> kafkaSinkBuilder = KafkaSink.<Row>builder()
                .setKafkaProducerConfig(new Properties());
//            .setRecordSerializer(schema)
        KafkaSink<Row> kafkaSink = kafkaSinkBuilder.build();


        for (Rule rule : rules) {
            Table table = tenv.sqlQuery("select * from fts_catalog." + rule.getDatabaseName() + "." + rule.getTableName());
            DataStream<Row> dataStream = tenv.toChangelogStream(table);

            FilterFunction<Row> function = null;
            switch (rule.getKind()) {
                case "INSERT":
                    function = new InsertFilter();
                    break;
                case "UPDATE":
                    function = new UpdateFilter();
                    break;
                case "DELETE":
                    function = new DeleteFilter();
                    break;
            }

            DataStream<Row> dataStream1 = dataStream.filter(function);

            //update消息需要对比下是哪些字段发生了变更
            if (rule.getKind().equals("UPDATE")) {
                dataStream1 = dataStream1.process(new ProcessFunction<Row, Row>() {
                    @Override
                    public void processElement(Row row, ProcessFunction<Row, Row>.Context context, Collector<Row> collector) throws Exception {

                    }
                });
            }


            //构建相应的消息体，发送kafka
            dataStream1.sinkTo(kafkaSink);

            dataStream1.print();
        }

        env.execute();
    }


    public static class Rule {
        //要监控的数据库
        private String databaseName;
        //要监控的表
        private String tableName;
        //要监控的类型，枚举值：INSERT，UPDATE,DELETE
        private String kind;

        //要监控的字段，（只有kind为UPDATE的时候有意义，即监控哪些字段发生了变化）
        private String[] fields;
        //规则的名称，同时作为kafka的key，保证单规则的数据的顺序是有序的。
        private String rule;

        public Rule(String databaseName, String tableName, String kind, String[] fields, String rule) {
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.kind = kind;
            this.fields = fields;
            this.rule = rule;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public String[] getFields() {
            return fields;
        }

        public void setFields(String[] fields) {
            this.fields = fields;
        }

        public String getRule() {
            return rule;
        }

        public void setRule(String rule) {
            this.rule = rule;
        }
    }

    public static class InsertFilter implements FilterFunction<Row> {
        @Override
        public boolean filter(Row row) {
            return row.getKind().equals(RowKind.INSERT);
        }
    }

    public static class UpdateFilter implements FilterFunction<Row> {
        @Override
        public boolean filter(Row row) {
            return row.getKind().equals(RowKind.UPDATE_AFTER) || row.getKind().equals(RowKind.UPDATE_BEFORE);
        }
    }

    public static class DeleteFilter implements FilterFunction<Row> {
        @Override
        public boolean filter(Row row) {
            return row.getKind().equals(RowKind.DELETE);
        }
    }
}
