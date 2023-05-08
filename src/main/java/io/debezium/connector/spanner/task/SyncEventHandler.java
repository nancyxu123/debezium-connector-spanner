/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.task.state.SyncEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.function.BlockingConsumer;

/**
 * Provides a logic for processing Sync Events of different types:
 * New Epoch, Rebalance Answers, Regular events
 */
public class SyncEventHandler {
    private static final Logger LOGGER = getLogger(SyncEventHandler.class);

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskSyncPublisher taskSyncPublisher;

    private final BlockingConsumer<TaskStateChangeEvent> eventConsumer;

    public SyncEventHandler(TaskSyncContextHolder taskSyncContextHolder, TaskSyncPublisher taskSyncPublisher,
                            BlockingConsumer<TaskStateChangeEvent> eventConsumer) {
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.taskSyncPublisher = taskSyncPublisher;
        this.eventConsumer = eventConsumer;
    }

    public void updateCurrentOffset(TaskSyncEvent inSync, SyncEventMetadata metadata) {
        if (inSync == null) {
            return;
        }
        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        boolean isLocked = taskSyncContextHolder.isLocked();
        String holder = taskSyncContextHolder.getHolder();
        int holdCount = taskSyncContextHolder.getHoldCount();
        boolean isHeldByCurrentThread = taskSyncContextHolder.isHeldByCurrentThread();
        Instant now = Instant.now();

        taskSyncContextHolder.update(oldContext -> oldContext.toBuilder()
                .currentKafkaRecordOffset(metadata.getOffset())
                .build());
        Instant after = Instant.now();
        LOGGER.warn(
                "With task {}, checking SyncEventHandler Time updating current offset: {} and was locked {} holder thread {}, hold count {}, heldByCurrentThread {}",
                taskSyncContextHolder.get().getTaskUid(),
                after.toEpochMilli() - now.toEpochMilli(),
                isLocked,
                holder,
                holdCount,
                isHeldByCurrentThread);

        LOGGER.debug("Task {} - update task sync topic offset with {}", taskSyncContextHolder.get().getTaskUid(), metadata.getOffset());
    }

    public void processPreviousStates(TaskSyncEvent inSync, SyncEventMetadata metadata) {

        if (!RebalanceState.START_INITIAL_SYNC.equals(taskSyncContextHolder.get().getRebalanceState())) {
            return;
        }

        process(inSync, metadata);

        if (metadata.isCanInitiateRebalancing()) {
            LOGGER.debug("Task {} - processPreviousStates - switch state to INITIAL_INCREMENTED_STATE_COMPLETED",
                    taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> context.toBuilder()
                    .rebalanceState(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)
                    .epochOffsetHolder(context.getEpochOffsetHolder().nextOffset(context.getCurrentKafkaRecordOffset()))
                    .build());
        }
    }

    public void processNewEpoch(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {
        taskSyncContextHolder.lock();
        try {

            long currentGeneration = taskSyncContextHolder.get().getRebalanceGenerationId();
            long inGeneration = inSync.getRebalanceGenerationId();

            if (taskSyncContextHolder.get().getRebalanceState() == RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED &&
                    inGeneration >= currentGeneration) { // We ignore messages with a stale rebalanceGenerationid.

                LOGGER.info("Task {} - processNewEpoch : {} metadata {}, rebalanceId: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        inSync,
                        metadata,
                        taskSyncContextHolder.get().getRebalanceGenerationId());

                taskSyncContextHolder.update(context -> SyncEventMerger.mergeNewEpoch(context, inSync));

                LOGGER.info("Task {} - SyncEventHandler sent response for new epoch",
                        taskSyncContextHolder.get().getTaskUid());

                taskSyncPublisher.send(taskSyncContextHolder.get().buildCurrentTaskSyncEvent());
            }
        }
        finally {
            taskSyncContextHolder.unlock();
        }

    }

    public void processUpdateEpoch(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {

        taskSyncContextHolder.lock();
        try {

            if (!taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED)) {
                return;
            }

            LOGGER.info("Task {} - process epoch update", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeEpochUpdate(context, inSync));

            eventConsumer.accept(new SyncEvent());
        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void processRegularMessage(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {
        Instant beginningRegularMessage = Instant.now();
        boolean wasLocked = taskSyncContextHolder.isLocked();
        String holder = taskSyncContextHolder.getHolder();
        int holdCount = taskSyncContextHolder.getHoldCount();
        boolean isHeldByCurrentThread = taskSyncContextHolder.isHeldByCurrentThread();

        taskSyncContextHolder.lock();

        Instant afterLocking = Instant.now();
        LOGGER.warn(
                "With task {}, checking SyncEventHandler processRegularMessage locking had lag {} with message type: {}, message size: {}, and wasLocked {} holder thread {}, hold count {}, heldByCurrentThread {}",
                taskSyncContextHolder.get().getTaskUid(),
                afterLocking.toEpochMilli() - beginningRegularMessage.toEpochMilli(),
                inSync.getMessageType(),
                inSync.toString().length(),
                wasLocked,
                holder,
                holdCount,
                isHeldByCurrentThread);
        try {

            if (!taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED)) {
                return;
            }

            LOGGER.debug("Task {} - process sync event", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeIncrementalTaskSyncEvent(context, inSync));

            eventConsumer.accept(new SyncEvent());

            Instant afterConsuming = Instant.now();
            LOGGER.warn(
                    "With task {}, checking SyncEventHandler processRegularMessage overall had lag {} with message type: {}, message size: {}, and wasLocked {}, holder thread {}, hold count {}, heldByCurrentThread {}",
                    taskSyncContextHolder.get().getTaskUid(),
                    afterConsuming.toEpochMilli() - beginningRegularMessage.toEpochMilli(),
                    inSync.getMessageType(),
                    inSync.toString().length(),
                    wasLocked,
                    holder,
                    holdCount,
                    isHeldByCurrentThread);

        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void processRebalanceAnswer(TaskSyncEvent inSync, SyncEventMetadata metadata) {

        taskSyncContextHolder.lock();

        try {

            if (!taskSyncContextHolder.get().isLeader() ||
                    !taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)) {
                return;
            }

            LOGGER.info("Task {} - process sync event - rebalance answer", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeRebalanceAnswer(context, inSync));

        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void process(TaskSyncEvent inSync, SyncEventMetadata metadata) {
        if (inSync == null) {
            return;
        }

        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        try {
            Instant now = Instant.now();
            if (inSync.getMessageType() == MessageTypeEnum.REGULAR) {
                Instant regularMessageBegin = Instant.now();
                processRegularMessage(inSync, metadata);
                Instant regularMessageEnd = Instant.now();
                LOGGER.warn("Task {}, regular message took {} to process with message type: {},  message size: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        regularMessageEnd.toEpochMilli() - regularMessageBegin.toEpochMilli(),
                        inSync.getMessageType(),
                        inSync.toString().length());
            }
            else if (inSync.getMessageType() == MessageTypeEnum.REBALANCE_ANSWER) {
                Instant rebalanceAnswerBegin = Instant.now();
                processRebalanceAnswer(inSync, metadata);
                Instant rebalanceAnswerEnd = Instant.now();
                LOGGER.warn("Task {}, rebalance message took {} to process with message type: {},  message size: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        rebalanceAnswerEnd.toEpochMilli() - rebalanceAnswerBegin.toEpochMilli(),
                        inSync.getMessageType(),
                        inSync.toString().length());
            }
            else if (inSync.getMessageType() == MessageTypeEnum.UPDATE_EPOCH) {
                Instant updateEpochBegin = Instant.now();
                processUpdateEpoch(inSync, metadata);
                Instant updateEpochEnd = Instant.now();
                LOGGER.warn("Task {}, update epoch message took {} to process with message type: {},  message size: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        updateEpochEnd.toEpochMilli() - updateEpochBegin.toEpochMilli(),
                        inSync.getMessageType(),
                        inSync.toString().length());
            }
            else if (inSync.getMessageType() == MessageTypeEnum.NEW_EPOCH) {
                Instant newEpochBegin = Instant.now();
                processNewEpoch(inSync, metadata);
                Instant newEpochEnd = Instant.now();
                LOGGER.warn("Task {}, new epoch message took {} to process with message type: {},  message size: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        newEpochEnd.toEpochMilli() - newEpochBegin.toEpochMilli(),
                        inSync.getMessageType(),
                        inSync.toString().length());
            }
            Instant end = Instant.now();
            if (end.toEpochMilli() - now.toEpochMilli() > 1000) {
                LOGGER.warn("Task {} Took very long to process this with message type: {} and message size: {} and lag {}", taskSyncContextHolder.get().getTaskUid(),
                        inSync.getMessageType(),
                        inSync.toString().length(),
                        end.toEpochMilli() - now.toEpochMilli());
            }
        }

        catch (Exception e) {
            LOGGER.error("Exception during processing task message {}, {}", inSync, e);
        }
    }

    private boolean skipFromPreviousGeneration(TaskSyncEvent inSync) {
        if (inSync != null) {
            long inGeneration = inSync.getRebalanceGenerationId();
            long currentGeneration = taskSyncContextHolder.get().getRebalanceGenerationId();

            if (inGeneration < currentGeneration) {
                LOGGER.debug("skipFromPreviousGeneration: currentGen: {}, inGen: {}, inTaskUid: {}", currentGeneration, inGeneration, inSync.getTaskUid());
                return true;
            }
        }
        return false;
    }
}
