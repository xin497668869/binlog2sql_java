package com.seewo.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.seewo.binlogsql.vo.RowVo;
import com.seewo.binlogsql.vo.TableVo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

import static com.seewo.binlogsql.tool.SqlGenerateTool.changeToRowVo;
import static com.seewo.binlogsql.tool.SqlGenerateTool.getComment;
import static com.seewo.binlogsql.tool.SqlGenerateTool.updateSql;
import static com.seewo.binlogsql.tool.TableTool.getTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class UpdateHandle implements BinlogEventHandle {

    @Override
    public List<String> handle(Event event, boolean isTurn) {

        UpdateRowsEventData updateRowsEventData = event.getData();
        TableVo tableVoInfo = getTableInfo(updateRowsEventData.getTableId());
        List<Pair> updateRows = updateRowsEventData.getRows().stream().map(entry -> {
            RowVo key = changeToRowVo(tableVoInfo, entry.getKey());
            RowVo value = changeToRowVo(tableVoInfo, entry.getValue());
            return new Pair(key, value);
        }).collect(Collectors.toList());

        if (isTurn) {
            List<Pair> reversedPairs = updateRows.stream()
                    .map(rowPair -> new Pair(rowPair.getAfter(), rowPair.getBefore()))
                    .collect(Collectors.toList());
            return updateSql(tableVoInfo, reversedPairs, getComment(event.getHeader()));
        } else {
            return updateSql(tableVoInfo, updateRows, getComment(event.getHeader()));
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Pair {
        private RowVo before;
        private RowVo after;
    }


}
