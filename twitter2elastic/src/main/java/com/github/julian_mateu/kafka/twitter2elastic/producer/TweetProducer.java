package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.ProducerFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReader;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReaderFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.TweetParser;
import com.google.inject.Inject;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Producer that reads a message from the Twitter API {@link TwitterMessageReader} and sends it to the
 * {@link MessageProcessor}.
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__({@Inject}))
public class TweetProducer {

    @NonNull
    private final ResourceManager resourceManager;

    /**
     * Reads a message from the {@link TwitterMessageReader} and sends it to the {@link MessageProcessor}
     *
     * @return An {@link Optional} instance of a {@link RecordMetadata} {@link Future}
     */
    static Optional<Future<RecordMetadata>> readAndProduce(TwitterMessageReader reader,
                                                           MessageProcessor messageProcessor) {
        return reader.readMessage()
                .flatMap(sendToProcessor(messageProcessor));
    }

    static Function<String, Optional<Future<RecordMetadata>>> sendToProcessor(MessageProcessor messageProcessor) {
        return (content) -> {
            log.info(content);
            return messageProcessor.processMessage(content);
        };
    }

    /**
     * Runs the Producer application.
     */
    @SneakyThrows({ExecutionException.class, InterruptedException.class, Exception.class})
    public void run(int numberOfMessagesToWrite) {

        @Cleanup TwitterMessageReader reader = resourceManager.getReader();
        @Cleanup MessageProcessor messageProcessor = resourceManager.getMessageProcessor();

        int numberOfMessagesWrittenSoFar = 0;
        while (numberOfMessagesWrittenSoFar < numberOfMessagesToWrite) {
            Optional<Future<RecordMetadata>> recordMetadataFuture = readAndProduce(reader, messageProcessor);
            if (recordMetadataFuture.isPresent()) {
                recordMetadataFuture.get().get();
                numberOfMessagesWrittenSoFar++;
            }
        }

        log.debug("processed {} messages", numberOfMessagesWrittenSoFar);
    }

    public interface ResourceManager {
        TwitterMessageReader getReader();

        MessageProcessor getMessageProcessor();
    }

    @RequiredArgsConstructor(onConstructor = @__({@Inject}))
    public static class SimpleResourceManager implements ResourceManager {

        @NonNull
        private final TwitterMessageReaderFactory twitterMessageReaderFactory;
        @NonNull
        private final ProducerFactory producerFactory;
        @NonNull
        private final TweetParser tweetParser;

        @Override
        public TwitterMessageReader getReader() {
            return twitterMessageReaderFactory.getTwitterMessageReader();
        }

        @Override
        public MessageProcessor getMessageProcessor() {
            return new MessageProcessor(producerFactory.getProducer(), tweetParser);
        }
    }
}
