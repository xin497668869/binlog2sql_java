package com.seewo.binlog2sql;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.List;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public interface BinlogEventHandle {

    List<String> handle(Event event, boolean isTurn) ;
}
