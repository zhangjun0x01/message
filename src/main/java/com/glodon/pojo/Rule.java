package com.glodon.pojo;

public class Rule {


    private String databaseName;   //要监控的数据库
    //要监控的表
    private String tableName;

    private MonitorType monitorType;     //要监控的类型，枚举值：INSERT，UPDATE,DELETE

    private String[] monitorFields;      //要监控的字段，（只有kind为UPDATE的时候有意义，即监控哪些字段发生了变化）
    private String[] messageFields;      //消息字段，要发送的消息字段

    private String selectFields; // 要查询的字段 ,逗号分隔

    private String rule;  //规则的名称，同时作为kafka的key，保证单规则的数据的顺序是有序的。

    private String primaryKeys;

    private Sink sink;

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public MonitorType getMonitorType() {
        return monitorType;
    }

    public String[] getMonitorFields() {
        return monitorFields;
    }

    public String[] getMessageFields() {
        return messageFields;
    }

    public String getSelectFields() {
        return selectFields;
    }

    public String getRule() {
        return rule;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public Sink getSink() {
        return sink;
    }
}