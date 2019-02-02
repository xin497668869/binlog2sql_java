package com.seewo.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.seewo.binlogsql.vo.EventFilterVo;
import com.seewo.binlogsql.vo.RowVo;
import com.seewo.binlogsql.vo.TableVo;

import java.util.Collections;
import java.util.List;

import static com.seewo.binlogsql.tool.SqlGenerateTool.changeToRowVo;
import static com.seewo.binlogsql.tool.SqlGenerateTool.deleteSql;
import static com.seewo.binlogsql.tool.SqlGenerateTool.getComment;
import static com.seewo.binlogsql.tool.SqlGenerateTool.insertSql;
import static com.seewo.binlogsql.tool.TableTool.getTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class DeleteHandle implements BinlogEventHandle {

    private final EventFilterVo eventFilterVo;

    public DeleteHandle(EventFilterVo eventFilterVo) {
        this.eventFilterVo = eventFilterVo;
    }

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        DeleteRowsEventData deleteRowsEventData = event.getData();
        TableVo tableVoInfo = getTableInfo(deleteRowsEventData.getTableId());

        if(!eventFilterVo.filter(tableVoInfo)) {
            return Collections.emptyList();
        }

        List<RowVo> rows = changeToRowVo(tableVoInfo, deleteRowsEventData.getRows());

        if (isTurn) {
            return insertSql(tableVoInfo, rows, getComment(event.getHeader()));
        } else {
            return deleteSql(tableVoInfo, rows, getComment(event.getHeader()));
        }
    }

}
