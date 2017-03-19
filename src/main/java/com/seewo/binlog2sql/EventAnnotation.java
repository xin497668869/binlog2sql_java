package com.seewo.binlog2sql;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EventAnnotation {
    String[] value();
}
