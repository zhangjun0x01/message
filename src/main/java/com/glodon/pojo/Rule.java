package com.glodon.pojo;

import java.util.Arrays;

public class Rule implements java.io.Serializable {
    private String tenantId;
    private String cloudtProjectid;
    private String zpertCompanyId;
    private String zpertItemId;
    private String zpertPlanId;
    private String databaseName;   //要监控的数据库
    //要监控的表
    private String tableName;
    private String[] monitorFields;      //要监控的字段，（只有kind为UPDATE的时候有意义，即监控哪些字段发生了变化）
    private String[] messageFields;      //消息字段，要发送的消息字段

    private String selectFields; // 要查询的字段 ,逗号分隔

    private String rule;  //规则的名称，同时作为kafka的key，保证单规则的数据的顺序是有序的。

    private String primaryKeys;

    private Sink sink = Sink.ROCKETMQ;

    private String rocketTag; // 如果sink选择rocketmq，则需要指定tag

    public String getTenantId() {
        return tenantId;
    }

    public String getCloudtProjectid() {
        return cloudtProjectid;
    }

    public String getZpertCompanyId() {
        return zpertCompanyId;
    }

    public String getZpertItemId() {
        return zpertItemId;
    }

    public String getZpertPlanId() {
        return zpertPlanId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
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

    public String getRocketTag() {
        return rocketTag;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "tenantId='" + tenantId + '\'' +
                ", cloudtProjectid='" + cloudtProjectid + '\'' +
                ", zpertCompanyId='" + zpertCompanyId + '\'' +
                ", zpertItemId='" + zpertItemId + '\'' +
                ", zpertPlanId='" + zpertPlanId + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", monitorFields=" + Arrays.toString(monitorFields) +
                ", messageFields=" + Arrays.toString(messageFields) +
                ", selectFields='" + selectFields + '\'' +
                ", rule='" + rule + '\'' +
                ", primaryKeys='" + primaryKeys + '\'' +
                ", sink=" + sink +
                ", rocketTag='" + rocketTag + '\'' +
                '}';
    }
}