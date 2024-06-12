/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.utils;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes action after specified delay,
 * if this action will not be overridden by a new one.
 *
 * If during this delay, action is updated - old action,
 * will be cancelled and time of the delay will be started again.
 */
public class ResettableDelayedAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResettableDelayedAction.class);

    private final Duration timeOut;

    private volatile Thread thread;

    private final String name;

    public ResettableDelayedAction(String name, Duration timeOut) {
        this.timeOut = timeOut;
        this.name = name;
    }

    public void set(Runnable action) {
        this.clear();

        this.thread = new Thread(() -> {
            try {
                Thread.sleep(timeOut.toMillis());
                action.run();

                clear();
            }
            catch (InterruptedException e) {
                LOGGER.info("Interrupting resettabledelayedaction with name {} with exception {}", name, e);
                Thread.currentThread().interrupt();
            }
        }, "SpannerConnector-" + name);

        this.thread.start();
    }

    public void clear() {
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }
}
