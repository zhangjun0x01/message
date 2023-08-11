package com.glodon.pojo;

public class Params implements java.io.Serializable {

    private PaimonInfo paimonInfo;
    private KafkaInfo kafkaInfo;
    private RocketmqInfo rocketmqInfo;
    private Rule[] rules;

    public PaimonInfo getPaimonInfo() {
        return paimonInfo;
    }

    public KafkaInfo getKafkaInfo() {
        return kafkaInfo;
    }

    public RocketmqInfo getRocketmqInfo() {
        return rocketmqInfo;
    }

    public Rule[] getRules() {
        return rules;
    }
}
