package com.github.julian_mateu.kafka.twitter2elastic.producer.testutils;

import lombok.NonNull;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Dummy implementation of a {@link RecordMetadata} {@link Future} to allow mocking, given that {@link RecordMetadata}
 * is a final class and can't be mocked by Mockito.
 */
public class RecordMetadataFutureMock implements Future<RecordMetadata> {

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public RecordMetadata get() {
        return null;
    }

    @Override
    public RecordMetadata get(long timeout, @NonNull TimeUnit unit) {
        return null;
    }
}
