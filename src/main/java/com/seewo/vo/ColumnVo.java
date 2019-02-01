package com.seewo.vo;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
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
public class ColumnVo {
    private String name;
    private ColumnType columnType;
}
