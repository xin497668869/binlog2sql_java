package com.seewo.binlog2sql.eventhandle;

import com.google.code.or.binlog.BinlogEventV4;

import java.util.List;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public interface BinlogEventHandle {

    List<String> handle(BinlogEventV4 event, boolean isTurn) ;
}
