package com.seewo.binlogsql.vo;

import lombok.Data;

import java.util.List;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Data
public class RowVo {
    private List<ColumnItemDataVo> value;
}
