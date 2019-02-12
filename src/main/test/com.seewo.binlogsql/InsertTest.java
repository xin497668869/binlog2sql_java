package com.seewo.binlogsql;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.seewo.binlogsql.handler.BinlogEventHandle;
import com.seewo.binlogsql.handler.DeleteHandle;
import com.seewo.binlogsql.handler.InsertHandle;
import com.seewo.binlogsql.handler.UpdateHandle;
import com.seewo.binlogsql.vo.DbInfoVo;
import com.seewo.binlogsql.vo.EventFilterVo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Slf4j
public class InsertTest {

    public static final TableTestPairVo           COMMON_TABLE_PAIR = new TableTestPairVo("flashback_test_common", "test_flashback_test_common");
    public static       List<Map<String, Object>> beforeData        = new ArrayList<>();

    public static  List<SqlTestVo> sqlTestVos = new ArrayList<>();
    private static BinlogListenSql binlogListenSql;

    @BeforeEach
     void initAll() throws Exception {
        DbInfoVo dbInfoVo = new DbInfoVo();
        dbInfoVo.setHost("localhost");
        dbInfoVo.setPort(3306);
        dbInfoVo.setUsername("root");
        dbInfoVo.setPassword("root");
        EventFilterVo eventFilterVo = new EventFilterVo()
                .setStartTime(System.currentTimeMillis())
                .setDefaultInclude(false)
                .setIncludeDbNames(Collections.singletonList("test2"))
                .setIncludeTableNames(Collections.singletonList(COMMON_TABLE_PAIR.getOrgTableName()));
        binlogListenSql = new BinlogListenSql(dbInfoVo)
                .setEventFilterVo(eventFilterVo)
                .connectAndListen();
        injectProxy(binlogListenSql, sqlTestVos);
        Thread.sleep(3000);

        cleanData(COMMON_TABLE_PAIR);
        Thread.sleep(3000);
        sqlTestVos.clear();
        log.info("数据清除完毕");

        binlogListenSql.close();
    }

    public static void cleanData(TableTestPairVo tableTestPairVo) throws Exception {
        MysqlUtil.insertOrUpdate("DELETE FROM " + tableTestPairVo.getOrgTableName());
        MysqlUtil.insertOrUpdate("DELETE FROM " + tableTestPairVo.getTestTableName());
    }

    private static void injectProxy(BinlogListenSql binlogListenSql, List<SqlTestVo> sqlTestVos) {
        Map<EventType, BinlogEventHandle> handleRegisterMap = binlogListenSql.getBinlogParser().getHandleRegisterMap();
        for (EventType eventType : handleRegisterMap.keySet()) {
            BinlogEventHandle orgBinlogEventHandle = handleRegisterMap.get(eventType);
            if (orgBinlogEventHandle instanceof InsertHandle
                    || orgBinlogEventHandle instanceof DeleteHandle
                    || orgBinlogEventHandle instanceof UpdateHandle)
                handleRegisterMap.put(eventType, new ProxyBinlogEventHandle(orgBinlogEventHandle, sqlTestVos));
        }
    }

    @AfterEach
    private void close() {
        binlogListenSql.close();
        binlogListenSql = null;
    }

    private void saveBeforeDbDataTemporary(TableTestPairVo tableTestPairVo) throws Exception {
        List<Map<String, Object>> orgDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getOrgTableName() + " ");
        changeByteArrayData(orgDatas);
        beforeData.clear();
        beforeData.addAll(orgDatas);
    }

    @Test
    void myFirstTest() throws Exception {
        saveBeforeDbDataTemporary(COMMON_TABLE_PAIR);
        MysqlUtil.insertOrUpdateByFile("insertTest.sql");
        Thread.sleep(2000);
        for (SqlTestVo sqlTestVo : sqlTestVos) {
            for (String sql : sqlTestVo.getSqls()) {
                log.info("准备插入测试表 " + sql);
                MysqlUtil.insertOrUpdate(COMMON_TABLE_PAIR.replaceTableName(sql));
            }
        }
        comparedTableData(COMMON_TABLE_PAIR);

        log.info("准备逆向测试");
        for (SqlTestVo sqlTestVo : sqlTestVos) {
            for (String rollSql : sqlTestVo.getRollSqls()) {
                log.info("准备roll测试表 " + rollSql);
                MysqlUtil.insertOrUpdate(COMMON_TABLE_PAIR.replaceTableName(rollSql));
            }
        }
        comparedBeforeTableData(COMMON_TABLE_PAIR);
    }

    void myFirstTest2() throws Exception {
        saveBeforeDbDataTemporary(COMMON_TABLE_PAIR);
        MysqlUtil.insertOrUpdateByFile("insertTest.sql");
        Thread.sleep(2000);
        for (SqlTestVo sqlTestVo : sqlTestVos) {
            for (String sql : sqlTestVo.getSqls()) {
                log.info("准备插入测试表 " + sql);
                MysqlUtil.insertOrUpdate(COMMON_TABLE_PAIR.replaceTableName(sql));
            }
        }
        comparedTableData(COMMON_TABLE_PAIR);

        log.info("准备逆向测试");
        for (SqlTestVo sqlTestVo : sqlTestVos) {
            for (String rollSql : sqlTestVo.getRollSqls()) {
                log.info("准备roll测试表 " + rollSql);
                MysqlUtil.insertOrUpdate(COMMON_TABLE_PAIR.replaceTableName(rollSql));
            }
        }
        comparedBeforeTableData(COMMON_TABLE_PAIR);
    }

    public void changeByteArrayData(List<Map<String, Object>> datas) {
        for (Map<String, Object> data : datas) {
            for (String key : data.keySet()) {
                if (data.get(key) instanceof byte[]) {
                    data.put(key, DatatypeConverter.printHexBinary((byte[]) data.get(key)));
                }
            }
        }
    }

    private void comparedBeforeTableData(TableTestPairVo tableTestPairVo) throws Exception {
        List<Map<String, Object>> testDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getTestTableName() + " ");
        changeByteArrayData(testDatas);
        assertEquals(beforeData, testDatas);

    }

    private void comparedTableData(TableTestPairVo tableTestPairVo) throws Exception {
        List<Map<String, Object>> orgDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getOrgTableName() + " ");
        List<Map<String, Object>> testDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getTestTableName() + " ");
        changeByteArrayData(orgDatas);
        changeByteArrayData(testDatas);
        assertEquals(orgDatas, testDatas);

    }

}

