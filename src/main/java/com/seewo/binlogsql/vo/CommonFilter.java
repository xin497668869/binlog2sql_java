package com.seewo.binlogsql.vo;

import com.github.shyiko.mysql.binlog.event.Event;
import com.seewo.binlogsql.Filter;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Data
@Accessors(chain = true)
@Slf4j
public class CommonFilter implements Filter {
    private List<FilterDbTableVo> includeDbTableVos;
    private long                  startTime;


    @Override
    public boolean filter(TableVo tableVoInfo) {
        if (includeDbTableVos == null) {
            return true;
        }
        if (includeDbTableVos.isEmpty()) {
            log.warn("没有设置监听的数据库表");
            return false;
        }
        for (FilterDbTableVo includeDbTableVo : includeDbTableVos) {
            if (Objects.equals(tableVoInfo.getDbName(), includeDbTableVo.getDbName()) &&
                    Objects.equals(tableVoInfo.getTableName(), includeDbTableVo.getTableName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean filter(Event event) {
        //            log.info(startTime + "  " + event.getHeader().getTimestamp());
        return startTime <= event.getHeader().getTimestamp();
    }

}
