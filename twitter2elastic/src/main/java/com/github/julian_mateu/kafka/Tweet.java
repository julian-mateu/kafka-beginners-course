package com.github.julian_mateu.kafka;

import lombok.Value;

import java.util.Map;

@Value
public class Tweet {
    String id;
    Map<String, Object> payload;
}
