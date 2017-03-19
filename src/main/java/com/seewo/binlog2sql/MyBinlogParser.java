package com.seewo.binlog2sql;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventParser;
import com.google.code.or.binlog.impl.AbstractBinlogParser;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventParser;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.FormatDescriptionEventParser;
import com.google.code.or.binlog.impl.parser.GtidEventParser;
import com.google.code.or.binlog.impl.parser.IncidentEventParser;
import com.google.code.or.binlog.impl.parser.IntvarEventParser;
import com.google.code.or.binlog.impl.parser.QueryEventParser;
import com.google.code.or.binlog.impl.parser.RandEventParser;
import com.google.code.or.binlog.impl.parser.RotateEventParser;
import com.google.code.or.binlog.impl.parser.StopEventParser;
import com.google.code.or.binlog.impl.parser.TableMapEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.UserVarEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.XidEventParser;
import com.google.code.or.io.XInputStream;
import com.google.code.or.net.Transport;
import com.google.code.or.net.impl.packet.EOFPacket;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.OKPacket;
import com.seewo.binlog2sql.eventhandle.BinlogEventHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class MyBinlogParser extends AbstractBinlogParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyBinlogParser.class);
    private List<String> sqls = new ArrayList<>();
    //
    protected Transport transport;
    protected String binlogFileName;

    public Map<Integer, BinlogEventHandle> handleRegisterMap = new HashMap<>();


    /**
     *
     */
    public MyBinlogParser() {
        registerEventParser(new StopEventParser());
        registerEventParser(new RotateEventParser());
        registerEventParser(new IntvarEventParser());
        registerEventParser(new XidEventParser());
        registerEventParser(new RandEventParser());
        registerEventParser(new QueryEventParser());
        registerEventParser(new UserVarEventParser());
        registerEventParser(new IncidentEventParser());
        registerEventParser(new TableMapEventParser());
        registerEventParser(new WriteRowsEventParser());
        registerEventParser(new UpdateRowsEventParser());
        registerEventParser(new DeleteRowsEventParser());
        registerEventParser(new WriteRowsEventV2Parser());
        registerEventParser(new UpdateRowsEventV2Parser());
        registerEventParser(new DeleteRowsEventV2Parser());
        registerEventParser(new FormatDescriptionEventParser());
        registerEventParser(new GtidEventParser());
    }

    @Override
    protected void doStart() throws Exception {
        // NOP
    }

    @Override
    public void start() throws Exception {
        //
        if (!this.running.compareAndSet(false, true)) {
            return;
        }
        sqls.clear();
        doParse();
    }

    @Override
    protected void doStop(long timeout, TimeUnit unit) throws Exception {
        // NOP
    }

    /**
     *
     */
    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    public void registerHandle(BinlogEventHandle handle, Integer... eventTypes) {
        for (Integer eventType : eventTypes) {
            handleRegisterMap.put(eventType, handle);
        }
    }

    /**
     *
     */
    @Override
    protected void doParse() throws Exception {
        //
        final XInputStream is = this.transport.getInputStream();
        final AbstractBinlogParser.Context context = new AbstractBinlogParser.Context(this.binlogFileName);
        while (isRunning() && is.available() > 0) {
            try {
                // Parse packet
                final int packetLength = is.readInt(3);
                final int packetSequence = is.readInt(1);
                is.setReadLimit(packetLength); // Ensure the packet boundary

                //
                final int packetMarker = is.readInt(1);
                if (packetMarker != OKPacket.PACKET_MARKER) { // 0x00
                    if ((byte) packetMarker == ErrorPacket.PACKET_MARKER) {
                        final ErrorPacket packet = ErrorPacket.valueOf(packetLength, packetSequence, packetMarker, is);
                        throw new RuntimeException(packet.toString());
                    } else if ((byte) packetMarker == EOFPacket.PACKET_MARKER) {
                        final EOFPacket packet = EOFPacket.valueOf(packetLength, packetSequence, packetMarker, is);
                        throw new RuntimeException(packet.toString());
                    } else {
                        throw new RuntimeException("assertion failed, invalid packet marker: " + packetMarker);
                    }
                }

                // Parse the event header
                final BinlogEventV4HeaderImpl header = new BinlogEventV4HeaderImpl();
                header.setTimestamp(is.readLong(4) * 1000L);
                header.setEventType(is.readInt(1));
                header.setServerId(is.readLong(4));
                header.setEventLength(is.readInt(4));
                header.setNextPosition(is.readLong(4));
                header.setFlags(is.readInt(2));
                header.setBinlogFileName(this.binlogFileName);
                header.setTimestampOfReceipt(System.currentTimeMillis());
                if (isVerbose() && LOGGER.isInfoEnabled()) {
                    LOGGER.info("received an event, sequence: {}, header: {}", packetSequence, header);
                }

                // Parse the event body
                if (this.eventFilter != null && !this.eventFilter.accepts(header, context)) {
                    this.defaultParser.parse(is, header, context);
                } else {
                    BinlogEventParser parser = getEventParser(header.getEventType());
                    if (parser == null) parser = this.defaultParser;
                    try {
                        parser.parse(is, header, context);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // Ensure the packet boundary
                if (is.available() != 0) {
                    throw new RuntimeException("assertion failed, available: " + is.available() + ", event type: " + header.getEventType());
                }
            } finally {
                is.setReadLimit(0);
            }
        }
    }

    public BinlogEventListener getBinlogEventListener(boolean isTurn) {
        return event -> {
            sqls.addAll(handleRegisterMap.getOrDefault(event.getHeader().getEventType(), (event1, isTurn1) -> Collections.emptyList())
                                         .handle(event, isTurn));
        };
    }

    public List<String> getSqls() {
        return sqls;
    }
}
