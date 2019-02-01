package com.seewo.binlogsql.tool;

import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.seewo.binlogsql.handler.UpdateHandle;
import com.seewo.binlogsql.vo.ColumnItemDataVo;
import com.seewo.binlogsql.vo.ColumnVo;
import com.seewo.binlogsql.vo.RowVo;
import com.seewo.binlogsql.vo.TableVo;

import javax.xml.bind.DatatypeConverter;
import java.io.Serializable;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
public class SqlGenerateTool {
    public static final ZoneId            UTC              = ZoneId.of("UTC");
    public static       DateTimeFormatter dateTimeFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
        if (itemDataVo.getValue() == null) {
            return "null";
        }
        switch (itemDataVo.getColumn().getJdbcType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case NUMERIC:
            case DECIMAL:
                return String.valueOf(itemDataVo.getValue());
            case CHAR:
            case VARCHAR:
                return with(String.valueOf(itemDataVo.getValue()));
            case TIME:
                Time time = (Time) itemDataVo.getValue();
                return with(LocalDateTime.ofInstant(Instant.ofEpochMilli(time.getTime()), UTC).format(DateTimeFormatter.ISO_LOCAL_TIME));
            case LONGVARCHAR:
            case LONGNVARCHAR:
                return with(new String((byte[]) itemDataVo.getValue()));
            case TIMESTAMP:
                //mysqlbinlog 的一个bug时间处理不对, 导致这里要特殊处理
                if (ColumnType.DATETIME_V2.equals(itemDataVo.getColumn().getColumnType())) {
                    return with(formatDateTime((Date) itemDataVo.getValue(), UTC));
                } else if (ColumnType.TIMESTAMP_V2.equals(itemDataVo.getColumn().getColumnType())) {
                    return with(formatDateTime((Date) itemDataVo.getValue(), ZoneId.systemDefault()));
                } else {
                    throw new RuntimeException("不支持的类型 " + itemDataVo.getColumn().getColumnType());
                }
            case DATE:
                return with(String.valueOf(itemDataVo.getValue()));
            case BLOB:
            case LONGVARBINARY:
                return "0x" + DatatypeConverter.printHexBinary((byte[]) itemDataVo.getValue());
            default:
                throw new RuntimeException("不能识别的类型 " + itemDataVo);
        }
    }

    private static String formatDateTime(Date date, ZoneId zoneOffset) {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        return dateTimeFormater.format(LocalDateTime.ofInstant(instant, zoneOffset));
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
                , formatDateTime(new Date(eventHeader.getTimestamp()), ZoneOffset.systemDefault()));
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
