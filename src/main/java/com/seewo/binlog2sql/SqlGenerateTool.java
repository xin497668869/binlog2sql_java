package com.seewo.binlog2sql;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.google.code.or.common.glossary.column.BlobColumn;
import com.google.code.or.common.glossary.column.DateColumn;
import com.google.code.or.common.glossary.column.Datetime2Column;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.google.code.or.common.glossary.column.DecimalColumn;
import com.google.code.or.common.glossary.column.DoubleColumn;
import com.google.code.or.common.glossary.column.FloatColumn;
import com.google.code.or.common.glossary.column.Int24Column;
import com.google.code.or.common.glossary.column.LongColumn;
import com.google.code.or.common.glossary.column.LongLongColumn;
import com.google.code.or.common.glossary.column.NullColumn;
import com.google.code.or.common.glossary.column.ShortColumn;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.glossary.column.Time2Column;
import com.google.code.or.common.glossary.column.TimeColumn;
import com.google.code.or.common.glossary.column.Timestamp2Column;
import com.google.code.or.common.glossary.column.TimestampColumn;
import com.google.code.or.common.glossary.column.TinyColumn;
import com.google.code.or.common.glossary.column.YearColumn;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class SqlGenerateTool {

    public static SimpleDateFormat formatTimestamp = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    private static String getUpdateSetCondition(Table tableInfo, Row row) {
        List<String> whereCondition = new ArrayList<>();

        for (int i = 0; i < row.getColumns().size(); i++) {
            String item = String.format("`%s` = %s", tableInfo.getColumns().get(i), getStringByColumnValue(row.getColumns().get(i)));
            if (item != null) {
                whereCondition.add(item);
            }
        }
        return whereCondition.stream().collect(Collectors.joining(" , "));
    }

    private static String getCondition(Table tableInfo, Row row) {
        List<String> whereCondition = new ArrayList<>();

        for (int i = 0; i < row.getColumns().size(); i++) {
            String item = combineItems(tableInfo.getColumns().get(i), row.getColumns().get(i));
            if (item != null) {
                whereCondition.add(item);
            }
        }
        return whereCondition.stream().collect(Collectors.joining(" AND "));
    }

    /**
     * 构造update的sql
     *
     * @param tableInfo 表信息, 包含数据库名, 表名, 列名
     * @param rowPairs  binlog中多行, 每行有对应列的值
     * @return
     */
    public static List<String> updateSql(Table tableInfo, List<Pair<Row>> rowPairs, String comment) {
        List<String> sqls = new ArrayList<>();
        for (Pair<Row> rowPair : rowPairs) {
            Row before = rowPair.getBefore();
            Row after = rowPair.getAfter();
            String setCondition = getUpdateSetCondition(tableInfo, after);
            String whereCondition = getCondition(tableInfo, before);
            String template = String.format("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;  #%s"
                    , tableInfo.getDbName()
                    , tableInfo.getTableName()
                    , setCondition
                    , whereCondition
                    , comment);

            sqls.add(template);

        }
        return sqls;
    }

    /**
     * 构造insert的sql
     *
     * @param tableInfo 表信息, 包含数据库名, 表名, 列名
     * @param rows      binlog中多行, 每行有对应列的值
     * @return
     */
    public static List<String> insertSql(Table tableInfo, List<Row> rows, String comment) {
        String insertCondition = tableInfo.getColumns().stream().map(columnName -> "`" + columnName + "`" ).collect(Collectors.joining(","));
        List<String> sqls = new ArrayList<>();
        for (Row row : rows) {
            String valueCondition = row.getColumns().stream().map(SqlGenerateTool::getStringByColumnValue).collect(Collectors.joining(","));
            String template = String.format("INSERT INTO `%s`.`%s`(%s) VALUES (%s); #%s"
                    , tableInfo.getDbName()
                    , tableInfo.getTableName()
                    , insertCondition
                    , valueCondition
                    , comment);
            sqls.add(template);
        }
        return sqls;
    }

    /**
     * 构造delete的sql
     *
     * @param tableInfo 表信息, 包含数据库名, 表名, 列名
     * @param rows      binlog中多行, 每行有对应列的值
     * @return
     */
    public static List<String> deleteSql(Table tableInfo, List<Row> rows, String comment) {
        List<String> sqls = new ArrayList<>();

        for (Row row : rows) {
            String whereCondition = getCondition(tableInfo, row);
            String template = String.format("DELETE FROM `%s`.`%s` WHERE %s LIMIT 1; #%s"
                    , tableInfo.getDbName()
                    , tableInfo.getTableName()
                    , whereCondition
                    , comment);
            sqls.add(template);
        }
        return sqls;
    }

    public static String with(String value) {
        return "'" + value + "'";
    }

    public static String getStringByColumnValue(Column column) {
        if (column instanceof BitColumn) {
            return with(column.toString());
        }else if (column instanceof StringColumn) {
            return with(column.toString());
        } else if (column instanceof DecimalColumn) {
            return column.toString();
        } else if (column instanceof ShortColumn) {
            return column.toString();
        } else if (column instanceof LongLongColumn) {
            return column.toString();
        } else if (column instanceof TinyColumn) {
            return column.toString();
        } else if (column instanceof LongColumn) {
            return column.toString();
        } else if (column instanceof Int24Column) {
            return column.toString();
        } else if (column instanceof DoubleColumn) {
            return column.toString();
        } else if (column instanceof FloatColumn) {
            return column.toString();
        } else if (column instanceof YearColumn) {
            return column.toString();
        } else if (column instanceof Time2Column) {
            return with(column.toString());
        } else if (column instanceof TimeColumn) {
            return with(column.toString());
        } else if (column instanceof DateColumn) {
            return with(column.toString());
        } else if (column instanceof Datetime2Column) {
            return with(formatTimestamp.format(column.getValue()));
        } else if (column instanceof DatetimeColumn) {
            return with(formatTimestamp.format(column.getValue()));
        } else if (column instanceof Timestamp2Column) {
            return with(column.toString());
        } else if (column instanceof TimestampColumn) {
            return with(column.toString());
        } else if (column instanceof BlobColumn) {
            return with(new String((byte[]) column.getValue()));
        } else if( column instanceof NullColumn) {
            return "null";
        }
        throw new RuntimeException("不能识别的类型 " + column);
    }

    public static String combineItems(String key, Column column) {

        if (column instanceof NullColumn) {
            return String.format("`%s` IS %s", key, getStringByColumnValue(column));
        } else {
            return String.format("`%s` = %s", key, getStringByColumnValue(column));
        }
    }
}
