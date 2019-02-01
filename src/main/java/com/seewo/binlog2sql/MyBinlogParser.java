package com.seewo.binlog2sql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
public class MyBinlogParser {

    public Map<EventType, BinlogEventHandle> handleRegisterMap = new HashMap<>();

    public void registerHandle(BinlogEventHandle handle, EventType... eventTypes) {
        for (EventType eventType : eventTypes) {
            handleRegisterMap.put(eventType, handle);
        }
    }

    public void handle(Event event) {
        BinlogEventHandle binlogEventHandle = handleRegisterMap.get(event.getHeader().getEventType());
        if(binlogEventHandle != null) {
            List<String> sql = binlogEventHandle.handle(event, false);
            System.out.println(sql);
        }
    }
}
