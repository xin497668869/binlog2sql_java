package com.seewo.binlogsql;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Data
@AllArgsConstructor
public class TableTestPairVo {
    private String orgTableName;
    private String testTableName;

    public String replaceTableName(String sql) {
        return sql.replace("`" + orgTableName + "`", "`" + testTableName + "`");
    }
}
