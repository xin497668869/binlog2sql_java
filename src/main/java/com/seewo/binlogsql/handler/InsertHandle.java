package com.seewo.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.seewo.binlogsql.Filter;
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

public class InsertHandle implements BinlogEventHandle {

    private final Filter filter;

    public InsertHandle(Filter filter) {
        this.filter = filter;
    }

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        WriteRowsEventData writeRowsEventV2 = event.getData();

        TableVo tableVoInfo = getTableInfo(writeRowsEventV2.getTableId());

        if(!filter.filter(tableVoInfo)) {
            return Collections.emptyList();
        }

       List<RowVo> rows = changeToRowVo(tableVoInfo, writeRowsEventV2.getRows());
        if (isTurn) {
            return deleteSql(tableVoInfo, rows, getComment(event.getHeader()));
        } else {
            return insertSql(tableVoInfo, rows, getComment(event.getHeader()));
        }

    }



}
