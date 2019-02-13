package com.seewo.binlogsql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.seewo.binlogsql.handler.BinlogEventHandle;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@AllArgsConstructor
public class ProxyBinlogEventHandle implements BinlogEventHandle {
    private BinlogEventHandle binlogEventHandle;
    private List<SqlTestVo>   sqlTestVos;

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        List<String> sql = binlogEventHandle.handle(event, false);
        List<String> rollSql = binlogEventHandle.handle(event, true);
        if (!sql.isEmpty()) {
            sqlTestVos.add(new SqlTestVo(sql, rollSql));
        }
        return sql;
    }
}
