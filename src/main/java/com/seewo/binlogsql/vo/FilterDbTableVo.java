package com.seewo.binlogsql.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FilterDbTableVo {
    private String dbName;
    private String tableName;
}
