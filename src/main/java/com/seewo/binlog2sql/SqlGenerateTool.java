package com.seewo.binlog2sql;

import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.seewo.binlog2sql.handler.UpdateHandle;
import com.seewo.vo.ColumnItemDataVo;
import com.seewo.vo.ColumnVo;
import com.seewo.vo.RowVo;
import com.seewo.vo.TableVo;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
public class SqlGenerateTool {
    public static SimpleDateFormat formatTimestamp = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    /**
     * 构造delete的sql
     *
     * @param tableInfo 表信息, 包含数据库名, 表名, 列名
     * @param rows      binlog中多行, 每行有对应列的值
     * @return
     */
    public static List<String> deleteSql(TableVo tableInfo, List<RowVo> rows, String comment) {
        List<String> sqls = new ArrayList<>();

        for (RowVo row : rows) {
            String whereCondition = getCondition(row);
            String template = String.format("DELETE FROM `%s`.`%s` WHERE %s LIMIT 1; #%s"
                    , tableInfo.getDbName()
                    , tableInfo.getTableName()
                    , whereCondition
                    , comment);
            sqls.add(template);
        }
        return sqls;
    }

    private static String getCondition(RowVo row) {
        List<String> whereCondition = new ArrayList<>();

        for (int i = 0; i < row.getValue().size(); i++) {
            String item = combineItems(row.getValue().get(i));
            if (item != null) {
                whereCondition.add(item);
            }
        }
        return String.join(" AND ", whereCondition);
    }

    /**
     * 构造insert的sql
     *
     * @param tableVoInfo 表信息, 包含数据库名, 表名, 列名
     * @param rows        binlog中多行, 每行有对应列的值
     * @return
     */
    public static List<String> insertSql(TableVo tableVoInfo, List<RowVo> rows, String comment) {
        String insertCondition = tableVoInfo.getColumns().stream().map(columnVo -> "`" + columnVo.getName() + "`").collect(Collectors.joining(","));
        List<String> sqls = new ArrayList<>();
        for (RowVo row : rows) {
            String valueCondition = row.getValue().stream().map(SqlGenerateTool::getStringByColumnValue).collect(Collectors.joining(","));
            String template = String.format("INSERT INTO `%s`.`%s`(%s) VALUES (%s); #%s"
                    , tableVoInfo.getDbName()
                    , tableVoInfo.getTableName()
                    , insertCondition
                    , valueCondition
                    , comment);
            sqls.add(template);
        }
        return sqls;
    }


    /**
     * 构造update的sql
     *
     * @param tableInfo 表信息, 包含数据库名, 表名, 列名
     * @param rowPairs  binlog中多行, 每行有对应列的值
     * @return
     */
    public static List<String> updateSql(TableVo tableInfo, List<UpdateHandle.Pair> rowPairs, String comment) {
        List<String> sqls = new ArrayList<>();
        for (UpdateHandle.Pair rowPair : rowPairs) {
            String setCondition = getUpdateSetCondition(rowPair.getAfter());
            String whereCondition = getCondition(rowPair.getBefore());
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


    private static String getUpdateSetCondition(RowVo rowVo) {
        List<String> whereCondition = new ArrayList<>();

        for (ColumnItemDataVo columnItemDataVo : rowVo.getValue()) {
            String item = String.format("`%s` = %s", columnItemDataVo.getColumn().getName(), getStringByColumnValue(columnItemDataVo));
            if (item != null) {
                whereCondition.add(item);
            }
        }
        return String.join(" , ", whereCondition);
    }

    public static String getStringByColumnValue(ColumnItemDataVo itemDataVo) {
        switch (itemDataVo.getColumn().getColumnType()) {
            case DATETIME:
            case DATETIME_V2:
            case TIMESTAMP_V2:
                return with(formatTimestamp.format(itemDataVo.getValue()));
            case YEAR:
            case NEWDATE:
            case BIT:
            case TIME_V2:
            case NEWDECIMAL:
            case ENUM:
            case SET:
            case GEOMETRY:
            case TINY:
            case SHORT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case NULL:
            case TIMESTAMP:
            case LONGLONG:
            case INT24:
            case TIME:
                return String.valueOf(itemDataVo.getValue());
            case DECIMAL:
            case DATE:
            case VAR_STRING:
            case VARCHAR:
            case JSON:
            case STRING:
                return with(String.valueOf(itemDataVo.getValue()));
            case TINY_BLOB:
            case MEDIUM_BLOB:
            case LONG_BLOB:
            case BLOB:
                return with(new String((byte[]) itemDataVo.getValue()));
            default:
                throw new RuntimeException("不能识别的类型 " + itemDataVo);
        }
    }

    public static String with(String value) {
        return "'" + value + "'";
    }

    public static String combineItems(ColumnItemDataVo itemDataVo) {
        if (ColumnType.NULL.equals(itemDataVo.getColumn().getColumnType())) {
            return String.format("`%s` IS %s", itemDataVo.getColumn().getName(), getStringByColumnValue(itemDataVo));
        } else {
            return String.format("`%s` = %s", itemDataVo.getColumn().getName(), getStringByColumnValue(itemDataVo));
        }
    }

    public static String getComment(EventHeaderV4 eventHeader) {
        return String.format("start %s end %s time %s"
                , eventHeader.getPosition()
                , eventHeader.getNextPosition()
                , formatTimestamp.format(eventHeader.getTimestamp()));
    }


    public static List<RowVo> changeToRowVo(TableVo tableVo, List<Serializable[]> rawRows) {
        List<RowVo> rowVos = new ArrayList<>();

        for (Serializable[] row : rawRows) {
            RowVo rowVo = changeToRowVo(tableVo, row);
            rowVos.add(rowVo);
        }
        return rowVos;
    }

    public static RowVo changeToRowVo(TableVo tableVo, Serializable[] row) {
        RowVo rowVo = new RowVo();
        List<ColumnItemDataVo> columnItemData = new ArrayList<>();
        rowVo.setValue(columnItemData);
        for (int i = 0; i < row.length; i++) {
            ColumnVo columnVo = tableVo.getColumns().get(i);
            columnItemData.add(new ColumnItemDataVo(row[i], columnVo));
        }
        return rowVo;
    }
}
