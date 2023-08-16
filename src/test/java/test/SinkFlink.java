package test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.sink.RocketMQSink;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

public class SinkFlink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.noRestart());


        DataStream<String> source =  env.fromElements("a","b");

        RocketMQSink<String> rocketmqSink =
                RocketMQSink.<String>builder()
                        .setEndpoints("127.0.0.1:9876")
                        .setSerializer(
                                (RocketMQSerializationSchema<String>)
                                        (element, context, timestamp) -> {
                                            return new Message(
                                                    "tx-mq-TOPIC",
                                                    "tag111",
                                                    element.getBytes());
                                        })
                         .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        // If you use transaction message, need set transaction timeout
//                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setConfig(
                                RocketMQSinkOptions.TRANSACTION_TIMEOUT,
                                TimeUnit.SECONDS.toSeconds(3))
                        .build();

        source.print();
        source.sinkTo(rocketmqSink);

        env.execute();
    }
}
