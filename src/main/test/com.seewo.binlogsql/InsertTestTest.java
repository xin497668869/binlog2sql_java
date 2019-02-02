package com.seewo.binlogsql;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static com.seewo.binlogsql.InsertTest.COMMON_TABLE_PAIR;
import static com.seewo.binlogsql.InsertTest.cleanData;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Slf4j
public class InsertTestTest {
    @Test
    void myFirstTest() throws Exception {

        cleanData(COMMON_TABLE_PAIR);
        log.info("!aaaaaaa!!!!!!!!!!!!!");
        log.info("!!!!!!!!!!!!!!");
        MysqlUtil.insertOrUpdateByFile("insertTest.sql");
        Thread.sleep(100000);
    }


}

