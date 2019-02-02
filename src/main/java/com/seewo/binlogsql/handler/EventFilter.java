package com.seewo.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.function.Predicate;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
public interface EventFilter extends Predicate<Event> {
    @Override
    boolean test(Event event);
}
