package com.glodon.pojo;

import java.util.Arrays;

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

    @Override
    public String toString() {
        return "Params{" +
                "paimonInfo=" + paimonInfo +
                ", kafkaInfo=" + kafkaInfo +
                ", rocketmqInfo=" + rocketmqInfo +
                ", rules=" + Arrays.toString(rules) +
                '}';
    }
}
