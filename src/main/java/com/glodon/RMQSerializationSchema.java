package com.glodon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.glodon.pojo.Rule;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.rocketmq.common.message.Message;

import java.sql.Timestamp;
import java.util.Objects;

import static org.apache.flink.types.RowKind.UPDATE_AFTER;

public class RMQSerializationSchema implements RocketMQSerializationSchema<Tuple2<Row, Rule>> {

    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RMQSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public Message serialize(Tuple2<Row, Rule> element, RocketMQSinkContext context, Long timestamp) {
        Row row = element.f0;
        Rule rule = element.f1;

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("tenant_id", rule.getTenantId());
        objectNode.put("rule", rule.getRule());
        objectNode.put("tableName", rule.getTableName());
        objectNode.put("databaseName", rule.getDatabaseName());
        String type = row.getKind().equals(UPDATE_AFTER) ? "UPDATE" : row.getKind().toString();
        objectNode.put("type", type);
        objectNode.put("updated_at", new Timestamp(timestamp).toString());

        ObjectNode message = objectMapper.createObjectNode();
        String[] messageFields = rule.getMessageFields();
        for (String field : messageFields) {
            message.put(field, Objects.requireNonNull(row.getField(field)).toString());
        }
        objectNode.set("message", message);

        try {
            return new Message(
                    topic,
                    element.f1.getRocketTag(),
                    objectMapper.writeValueAsString(objectNode).getBytes());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
