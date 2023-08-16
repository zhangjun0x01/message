package com.glodon.pojo;

public class PaimonInfo {
    private String warehouse;
    private String uri;
    private String s3Endpoint;
    private String s3AccessKeyId;
    private String s3SecretAccessKey;


    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setS3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
    }

    public void setS3AccessKeyId(String s3AccessKeyId) {
        this.s3AccessKeyId = s3AccessKeyId;
    }

    public void setS3SecretAccessKey(String s3SecretAccessKey) {
        this.s3SecretAccessKey = s3SecretAccessKey;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public String getUri() {
        return uri;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3AccessKeyId() {
        return s3AccessKeyId;
    }

    public String getS3SecretAccessKey() {
        return s3SecretAccessKey;
    }

    @Override
    public String toString() {
        return "PaimonInfo{" +
                "warehouse='" + warehouse + '\'' +
                ", uri='" + uri + '\'' +
                ", s3Endpoint='" + s3Endpoint + '\'' +
                ", s3AccessKeyId='" + s3AccessKeyId + '\'' +
                ", s3SecretAccessKey='" + s3SecretAccessKey + '\'' +
                '}';
    }
}
