package com.lmd.kafka.stream.frauddetector.generator;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Random;

@Component
public class FaKeClock {
    private LocalDateTime current = LocalDateTime.now ().minusYears (2);
    private Random random = new Random ();

    public LocalDateTime now () {
        return current.plus (random.nextInt (1000), ChronoUnit.MILLIS);
    }
}
