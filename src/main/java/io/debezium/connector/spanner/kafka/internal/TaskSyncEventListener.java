/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.function.BlockingBiConsumer;
import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.kafka.internal.proto.SyncEventFromProtoMapper;

/** Consumes messages from the Sync Topic */
public class TaskSyncEventListener {
    private static final Logger LOGGER = getLogger(TaskSyncEventListener.class);
    private final String consumerGroup;
    private final String topic;
    private final boolean seekBackToPreviousEpoch;
    private final Duration pollDuration;
    private final Duration commitOffsetsTimeout;
    private final long commitOffsetsInterval;
    private final SyncEventConsumerFactory<String, byte[]> consumerFactory;
    private final List<BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata>> eventConsumers = new ArrayList<>();
    private final java.util.function.Consumer<RuntimeException> errorHandler;

    private volatile Thread thread;

    public TaskSyncEventListener(
                                 String consumerGroup,
                                 String topic,
                                 SyncEventConsumerFactory<String, byte[]> consumerFactory,
                                 boolean seekBackToPreviousEpoch,
                                 java.util.function.Consumer<RuntimeException> errorHandler) {

        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.seekBackToPreviousEpoch = seekBackToPreviousEpoch;
        this.pollDuration = Duration.ofMillis(consumerFactory.getConfig().syncPollDuration());
        this.commitOffsetsTimeout = Duration.ofMillis(consumerFactory.getConfig().syncCommitOffsetsTimeout());
        this.commitOffsetsInterval = consumerFactory.getConfig().syncCommitOffsetsInterval();
        this.consumerFactory = consumerFactory;
        this.errorHandler = errorHandler;
    }

    public void subscribe(BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer) {
        eventConsumers.add(eventConsumer);
    }

    public void unsubscribe(BiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer) {
        eventConsumers.remove(eventConsumer);
    }

    public void start() throws InterruptedException {
        // or take all partition list and sub to them

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        List<TopicPartition> assignment = List.of(topicPartition);

        Consumer<String, byte[]> consumer = consumerFactory.createConsumer(consumerGroup);
        consumer.assign(assignment);

        Long endOffset = consumer.endOffsets(assignment).get(topicPartition);
        Long beginOffset = consumer.beginningOffsets(assignment).get(topicPartition);

        long startOffset = Math.max(endOffset - 1, beginOffset);

        try {

            if (endOffset == startOffset) {
                LOGGER.debug("listen: Sync topic is empty, so initial sync is finished");
                for (BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer : eventConsumers) {
                    eventConsumer.accept(
                            null, SyncEventMetadata.builder().canInitiateRebalancing(true).build());
                }
            }
            else {
                LOGGER.debug("listen: read last message");
                try {
                    consumer.seek(topicPartition, startOffset);
                    seekBackToPreviousEpoch(consumer, topicPartition, beginOffset);
                }
                catch (org.apache.kafka.common.errors.InterruptException e) {
                    throw new InterruptedException();
                }
                catch (Exception e) {
                    errorHandler.accept(
                            new SpannerConnectorException("Error during seek back the Sync Topic", e));
                    return;
                }
            }

        }
        catch (Exception ex) {
            shutdownConsumer(consumer);
            throw ex;
        }

        thread = new Thread(
                () -> {
                    try {
                        long commitOffsetStart = System.currentTimeMillis();
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                poll(consumer, endOffset);
                                if (!consumerFactory.isAutoCommitEnabled()
                                        && commitOffsetStart + commitOffsetsInterval < System.currentTimeMillis()) {

                                    consumer.commitSync(commitOffsetsTimeout);
                                    commitOffsetStart = System.currentTimeMillis();
                                }
                            }
                            catch (org.apache.kafka.common.errors.InterruptException
                                    | InterruptedException ex) {
                                LOGGER.error("ERROR DURING POLL FROM SYNC TOPIC: {}", ex);
                                return;
                            }
                            catch (Exception e) {
                                LOGGER.error("ERROR DURING POLL FROM SYNC TOPIC: {}", e);
                                errorHandler.accept(
                                        new SpannerConnectorException("Error during poll from the Sync Topic", e));
                                return;
                            }
                        }

                    }
                    finally {
                        shutdownConsumer(consumer);
                    }
                },
                "SpannerConnector-TaskSyncEventListener");

        thread.start();
    }

    private int poll(Consumer<String, byte[]> consumer, long endOffset)
            throws InvalidProtocolBufferException, InterruptedException, Exception {

        Instant now = Instant.now();
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long beginningPosition = consumer.position(topicPartition);
        ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
        LOGGER.trace("listen: poll messages count: {}", records.count());

        if (records.isEmpty()) {
            return 0;
        }

        Instant processingTime = Instant.now();

        int recordBytes = 0;
        Instant beforeFetchingRecord = Instant.now();
        for (ConsumerRecord<String, byte[]> record : records) {

            Instant parsingBeginTime = Instant.now();
            TaskSyncEvent taskSyncEvent = parseSyncEvent(record);
            Instant parsingEndTime = Instant.now();
            debug(LOGGER, "Receive SyncEvent from Kafka topic: {}", taskSyncEvent);
            int serializedValueSize = record.serializedValueSize();
            recordBytes += serializedValueSize;

            int i = 0;
            for (BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer : eventConsumers) {
                Instant beginFetchingRecord = Instant.now();
                eventConsumer.accept(
                        taskSyncEvent,
                        SyncEventMetadata.builder()
                                .offset(record.offset())
                                // Once we have consumed all the messages present in the sync topic at the
                                // start of the connector, we can then connect to the rebalance topic.
                                .canInitiateRebalancing(record.offset() >= endOffset - 1)
                                .build());
                Instant computationEndTime = Instant.now();
                LOGGER.warn(
                        "With task {}, Polling iteration: {} Time parsing record {}, Time processing record: {}, record type {}, record bytes {} and event consumer {} and total event consumers {}",
                        consumerGroup,
                        now.toString(),
                        parsingEndTime.toEpochMilli() - parsingBeginTime.toEpochMilli(),
                        computationEndTime.toEpochMilli() - beginFetchingRecord.toEpochMilli(),
                        taskSyncEvent.getMessageType(), serializedValueSize, i, eventConsumers.size());
                i++;
            }
            // if (record.serializedValueSize() > 1000000) {
            // LOGGER.info("With task {}, Polling iteration: {} with large record {} with message type {} and parsing time {}, processing time {}, record fetch time {}",
            // consumerGroup,
            // now.toString(),
            // record.serializedValueSize(),
            // taskSyncEvent.getMessageType(), parsingEndTime.toEpochMilli() - parsingBeginTime.toEpochMilli(),
            // computationEndTime.toEpochMilli() - parsingEndTime.toEpochMilli(),
            // parsingBeginTime.toEpochMilli() - beforeFetchingRecord.toEpochMilli());
            // }
        }
        Instant end = Instant.now();
        if (end.toEpochMilli() - now.toEpochMilli() > 1000) {
            LOGGER.warn(
                    "With task {}, Polling iteration: {} long poll with long lag {}, fetch time {}, processing time {}, record count {}, record bytes {}, original position {}, ending position {}",
                    consumerGroup, now.toString(),
                    end.toEpochMilli() - now.toEpochMilli(),
                    processingTime.toEpochMilli() - now.toEpochMilli(),
                    end.toEpochMilli() - processingTime.toEpochMilli(),
                    records.count(),
                    recordBytes,
                    beginningPosition,
                    consumer.position(topicPartition));
        }
        return records.count();
    }

    private void seekBackToPreviousEpoch(
                                         Consumer<String, byte[]> consumer, TopicPartition topicPartition, long beginOffset)
            throws InvalidProtocolBufferException {
        if (!seekBackToPreviousEpoch) {
            return;
        }
        ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);

        if (records.isEmpty()) {
            LOGGER.warn("listen: fail to poll last message");
            return;
        }

        ConsumerRecord<String, byte[]> lastRecord = records.iterator().next();
        TaskSyncEvent taskSyncEvent = parseSyncEvent(lastRecord);

        long previousEpochOffset = taskSyncEvent.getEpochOffset();
        long startOffset = Math.max(previousEpochOffset, beginOffset);

        LOGGER.debug("listen: seek back to previous epoch offset: {}", startOffset);
        consumer.seek(topicPartition, startOffset);
    }

    private TaskSyncEvent parseSyncEvent(ConsumerRecord<String, byte[]> record)
            throws InvalidProtocolBufferException {
        return SyncEventFromProtoMapper.mapFromProto(
                SyncEventProtos.SyncEvent.parseFrom(record.value()));
    }

    private void shutdownConsumer(Consumer<String, byte[]> consumer) {
        try {
            consumer.unsubscribe();
            consumer.close();
        }
        catch (org.apache.kafka.common.errors.InterruptException e) {
            if (!Thread.currentThread().isInterrupted()) {
                LOGGER.info("Interrupting shutdownConsumer task {}", consumerGroup);
                Thread.currentThread().interrupt();
            }
        }
    }

    public void shutdown() {
        if (thread == null) {
            return;
        }
        LOGGER.info("shutdown task sync event listener {}", consumerGroup);
        thread.interrupt();

        while (!thread.getState().equals(Thread.State.TERMINATED)) {
        }
        thread = null;
    }
}
