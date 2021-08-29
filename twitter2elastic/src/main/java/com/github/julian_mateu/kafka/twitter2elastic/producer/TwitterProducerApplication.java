package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.Producer;
import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.ProducerFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReader;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReaderFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.TweetParser;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Application that launches a Kafka Producer which reads tweets from the Twitter API and loads them to a topic.
 */
@Slf4j
@RequiredArgsConstructor
public class TwitterProducerApplication {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "tweets";
    private static final int NUMBER_OF_MESSAGES_TO_WRITE = 5;

    @NonNull
    private final TweetProducer tweetProducer;

    public static void main(String[] args) {

        @Cleanup TwitterMessageReader twitterMessageReader = TwitterMessageReaderFactory.get();

        @Cleanup Producer producer = ProducerFactory.getProducer(BOOTSTRAP_SERVERS, TOPIC_NAME);
        TweetParser parser = new TweetParser(new ObjectMapper());
        MessageProcessor messageProcessor = new MessageProcessor(producer, parser);

        TweetProducer tweetProducer = new TweetProducer(twitterMessageReader, messageProcessor);
        TwitterProducerApplication application = new TwitterProducerApplication(tweetProducer);
        application.run();
    }

    /**
     * Runs the Producer application.
     */
    @SneakyThrows({ExecutionException.class, InterruptedException.class})
    public void run() {
        int numberOfMessagesWrittenSoFar = 0;

        while (numberOfMessagesWrittenSoFar < NUMBER_OF_MESSAGES_TO_WRITE) {
            Optional<Future<RecordMetadata>> recordMetadataFuture = tweetProducer.readAndProduce();
            if (recordMetadataFuture.isPresent()) {
                recordMetadataFuture.get().get();
                numberOfMessagesWrittenSoFar++;
            }
        }

        log.debug("processed {} messages", numberOfMessagesWrittenSoFar);
    }
}
