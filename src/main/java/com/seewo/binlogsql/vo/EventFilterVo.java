package com.seewo.binlogsql.vo;

import com.github.shyiko.mysql.binlog.event.EventType;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Data
@Accessors(chain = true)
public class EventFilterVo {
    private List<String>    includeDbNames;
    private List<String>    includeTableNames;
    private List<EventType> includeEventTypes;
    private List<String>    excludeDbNames;
    private List<String>    excludeTableNames;
    private List<EventType> excludeEventTypes;
    private long startTime;
    /**
     * 默认不包含的都包含
     */
    private boolean         defaultInclude = true;


    public boolean filter(TableVo tableVoInfo) {

        if (defaultInclude) {
            if (isContain(excludeDbNames, tableVoInfo.getDbName())) {
                return false;
            }
            if (isContain(excludeTableNames, tableVoInfo.getTableName())) {
                return false;
            }
        } else {
            if (isContain(includeDbNames, tableVoInfo.getDbName())) {
                return true;
            }
            if (isContain(includeTableNames, tableVoInfo.getTableName())) {
                return true;
            }
        }
        return defaultInclude;
    }

    private <T> boolean isContain(List<T> collection, T name) {
        return collection != null && collection.contains(name);
    }

}
