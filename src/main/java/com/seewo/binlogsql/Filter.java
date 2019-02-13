package com.seewo.binlogsql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.seewo.binlogsql.vo.TableVo;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
public interface Filter {
    default boolean filter(TableVo tableVoInfo) {
        return true;
    }

    default boolean filter(Event event) {
        return true;
    }
}
