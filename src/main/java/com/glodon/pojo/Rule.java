package com.glodon.pojo;

public class Rule {
    //要监控的数据库
    private String databaseName;
    //要监控的表
    private String tableName;
    //要监控的类型，枚举值：INSERT，UPDATE,DELETE
    private String kind;

    private String[] monitorFields;      //要监控的字段，（只有kind为UPDATE的时候有意义，即监控哪些字段发生了变化）
    private String[] messageFields;      //消息字段，要发送的消息字段

    private String selectFields; // 要查询的字段 ,逗号分隔

    private String rule;  //规则的名称，同时作为kafka的key，保证单规则的数据的顺序是有序的。

    private String primaryKeys;

    private Sink sink;

    public Rule(String databaseName, String tableName, String kind, String[] fields, String rule) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.kind = kind;
        this.monitorFields = fields;
        this.rule = rule;
    }

    public String getSelectFields() {
        return selectFields;
    }

    public void setSelectFields(String selectFields) {
        this.selectFields = selectFields;
    }

    public String[] getMessageFields() {
        return messageFields;
    }

    public void setMessageFields(String[] messageFields) {
        this.messageFields = messageFields;
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

    public String[] getMonitorFields() {
        return monitorFields;
    }

    public void setMonitorFields(String[] monitorFields) {
        this.monitorFields = monitorFields;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public Sink getSink() {
        return sink;
    }

    public void setSink(Sink sink) {
        this.sink = sink;
    }
}