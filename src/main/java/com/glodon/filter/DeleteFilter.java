package com.glodon.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public  class DeleteFilter implements FilterFunction<Row> {
        @Override
        public boolean filter(Row row) {
            return row.getKind().equals(RowKind.DELETE);
        }
    }