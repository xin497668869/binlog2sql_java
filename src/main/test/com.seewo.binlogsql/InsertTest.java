package com.seewo.binlogsql;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.seewo.binlogsql.handler.BinlogEventHandle;
import com.seewo.binlogsql.handler.DeleteHandle;
import com.seewo.binlogsql.handler.InsertHandle;
import com.seewo.binlogsql.handler.UpdateHandle;
import com.seewo.binlogsql.vo.DbInfoVo;
import com.seewo.binlogsql.vo.EventFilterVo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.seewo.binlogsql.BinlogListenSql.getRollBackSql;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Slf4j
public class InsertTest {

    public static final TableTestPairVo COMMON_TABLE_PAIR = new TableTestPairVo("flashback_test_common", "test_flashback_test_common");

    public static List<SqlTestVo> sqlTestVos = new ArrayList<>();


    @BeforeAll
    static void initAll() throws Exception {
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
        MyBinlogParser rollBackSql = getRollBackSql(dbInfoVo, eventFilterVo);

        injectProxy(rollBackSql, sqlTestVos);
        Thread.sleep(3000);
    }

    private static void injectProxy(MyBinlogParser rollBackSql, List<SqlTestVo> sqlTestVos) {
        Map<EventType, BinlogEventHandle> handleRegisterMap = rollBackSql.getHandleRegisterMap();
        for (EventType eventType : handleRegisterMap.keySet()) {
            BinlogEventHandle orgBinlogEventHandle = handleRegisterMap.get(eventType);
            if (orgBinlogEventHandle instanceof InsertHandle
                    || orgBinlogEventHandle instanceof DeleteHandle
                    || orgBinlogEventHandle instanceof UpdateHandle)
                handleRegisterMap.put(eventType, new ProxyBinlogEventHandle(orgBinlogEventHandle, sqlTestVos));
        }
    }

    public static void cleanData(TableTestPairVo tableTestPairVo) throws Exception {
        MysqlUtil.insertOrUpdate("DELETE FROM " + tableTestPairVo.getOrgTableName());
        MysqlUtil.insertOrUpdate("DELETE FROM " + tableTestPairVo.getTestTableName());
    }

    @BeforeEach
    private void before() throws Exception {
        cleanData(COMMON_TABLE_PAIR);
        sqlTestVos.clear();
        Thread.sleep(5000);
        log.info("数据清除完毕");
    }

    @Test
    void myFirstTest() throws Exception {
        MysqlUtil.insertOrUpdateByFile("insertTest.sql");
        Thread.sleep(1000);
        for (SqlTestVo sqlTestVo : sqlTestVos) {
            for (String sql : sqlTestVo.getSqls()) {
                log.info("准备插入测试表 " + sql);
                MysqlUtil.insertOrUpdate(COMMON_TABLE_PAIR.replaceTableName(sql));
            }
        }
        comparedTableData(COMMON_TABLE_PAIR);
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

    private void comparedTableData(TableTestPairVo tableTestPairVo) throws Exception {
        List<Map<String, Object>> orgDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getOrgTableName() + " ");
        List<Map<String, Object>> testDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getTestTableName() + " ");
        changeByteArrayData(orgDatas);
        changeByteArrayData(testDatas);
        assertEquals(orgDatas, testDatas);
//        for (int i = 0; i < orgDatas.size(); i++) {
//            assertEquals(orgDatas.get(i),testDatas.get(i));
//        }
    }

}

