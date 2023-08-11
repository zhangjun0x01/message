package com.glodon.filter;


import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Objects;

public class UpdateFilter extends RichFilterFunction<Row> {


    private transient MapState<String, Row> mapState;
    private final String tableName;
    private final String primaryKeys;

    private final String[] monitorFields;


    public UpdateFilter(String tableName, String primaryKeys, String[] monitorFields) {
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.monitorFields = monitorFields;
    }

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Row> descriptor = new MapStateDescriptor<>("my-map-state", String.class, Row.class);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public boolean filter(Row row) throws Exception {
        String key = tableName + "-" + primaryKeys;
        if (row.getKind().equals(RowKind.UPDATE_BEFORE)) {
            mapState.put(key, row);
        }

        if (row.getKind().equals(RowKind.UPDATE_AFTER)) {
            Row oldRow = mapState.get(key);
            if (oldRow != null) {
                mapState.remove(key);
                for (String field : monitorFields) {
                    return Objects.equals(oldRow.getField(field), row.getField(field));
                }
            }
        }
        return false;
    }
}