package com.seewo.binlogsql.test;

import com.seewo.binlogsql.MysqlUtil;
import com.seewo.binlogsql.SqlTest;
import com.seewo.binlogsql.TableTestPairVo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.RepeatedTest;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.RepeatedTest.CURRENT_REPETITION_PLACEHOLDER;
import static org.junit.jupiter.api.RepeatedTest.DISPLAY_NAME_PLACEHOLDER;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Slf4j
public class UpdateTest extends SqlTest {

    public TableTestPairVo COMMON_TABLE_PAIR = new TableTestPairVo("flashback_test_common", "test_flashback_test_common");

    @RepeatedTest(value = 3, name = DISPLAY_NAME_PLACEHOLDER + "  " + CURRENT_REPETITION_PLACEHOLDER)
    public void insertTest1() throws Exception {
        test("updateTestSingle.sql", COMMON_TABLE_PAIR);
    }

    @Override
    protected void initData() throws Exception {
        MysqlUtil.insertOrUpdateByFile("updateInitTestSingle.sql");
    }

    @Override
    protected List<TableTestPairVo> getTableTestPairVo() {
        return Arrays.asList(COMMON_TABLE_PAIR);
    }
}

