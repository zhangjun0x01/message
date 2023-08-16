package com.glodon.filter;


import com.glodon.pojo.Rule;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;

public class UpdateFilter extends RichFilterFunction<Tuple2<Row, Rule>> {


    private transient MapState<String, Row> mapState;
    private final String tableName;
    private final String[] monitorFields;


    public UpdateFilter(String tableName, String[] monitorFields) {
        this.tableName = tableName;
        this.monitorFields = monitorFields;
    }

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Row> descriptor = new MapStateDescriptor<>("my-map-state", String.class, Row.class);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public boolean filter(Tuple2<Row, Rule> tuple) throws Exception {
        Row row = tuple.f0;
        Rule rule = tuple.f1;
        String tableMame = rule.getTableName();

        if (row.getKind().equals(INSERT) || row.getKind().equals(DELETE)) {
            return true;
        }

        if (row.getKind().equals(RowKind.UPDATE_BEFORE)) {
            mapState.put(tableMame, row);
        }

        if (row.getKind().equals(RowKind.UPDATE_AFTER)) {
            Row oldRow = mapState.get(tableMame);
            if (oldRow != null) {
                mapState.remove(tableMame);
                return Arrays.stream(monitorFields).anyMatch(field -> !Objects.equals(oldRow.getField(field), row.getField(field)));
            }
        }

        return false;
    }

}