package com.seewo.binlogsql;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
public class InsertTest {

    @BeforeAll
    static void initAll() {
    }

    @Test
    void myFirstTest() throws Exception {
        List<Map<String, Object>> query = MysqlUtil.query("SELECT * FROM flashback_test_common WHERE bigint_ = 1111");
        System.out.println();
    }

}

