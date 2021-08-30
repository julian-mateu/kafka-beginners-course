package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Application that launches a Kafka Producer which reads tweets from the Twitter API and loads them to a topic.
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__({@Inject}))
public class TwitterProducerApplication {

    private static final int NUMBER_OF_MESSAGES_TO_WRITE = 5;

    @NonNull
    private final TweetProducer tweetProducer;

    public static void main(String[] args) {
        getInstance().run();
    }

    public static TwitterProducerApplication getInstance() {
        Injector injector = Guice.createInjector(new TwitterProducerApplicationModule());
        return injector.getInstance(TwitterProducerApplication.class);
    }

    public void run() {
        tweetProducer.run(NUMBER_OF_MESSAGES_TO_WRITE);
    }
}
