package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReader;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;
import java.util.concurrent.Future;

/**
 * Producer that reads a message from the Twitter API {@link TwitterMessageReader} and sends it to the
 * {@link MessageProcessor}.
 */
@Slf4j
@RequiredArgsConstructor
public class TweetProducer {

    @NonNull
    private final TwitterMessageReader reader;
    @NonNull
    private final MessageProcessor processor;

    /**
     * Reads a message from the {@link TwitterMessageReader} and sends it to the {@link MessageProcessor}
     *
     * @return An {@link Optional} instance of a {@link RecordMetadata} {@link Future}
     */
    public Optional<Future<RecordMetadata>> readAndProduce() {
        return reader.readMessage()
                .map(this::sendToProcessor);
    }

    private Future<RecordMetadata> sendToProcessor(String content) {
        log.info(content);
        return processor.processMessage(content);
    }
}
