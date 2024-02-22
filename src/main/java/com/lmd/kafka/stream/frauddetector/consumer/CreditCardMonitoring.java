package com.lmd.kafka.stream.frauddetector.consumer;

import com.lmd.kafka.stream.frauddetector.CreditCardLock;
import com.lmd.kafka.stream.frauddetector.CreditCardTransaction;
import com.lmd.kafka.stream.frauddetector.TransactionStatus;
import com.lmd.kafka.stream.frauddetector.generator.FaKeClock;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static java.lang.ProcessBuilder.Redirect.to;

@Component
public class CreditCardMonitoring {

    @Autowired
    private FaKeClock clock;
    Logger logger = org.apache.logging.log4j.LogManager.getLogger (CreditCardMonitoring.class);

    TimestampExtractor extractor = new TimestampExtractor () {
        @Override
        public long extract (ConsumerRecord<Object, Object> record, long previousTimestamp) {
            return ((CreditCardTransaction) record.value ()).getDateTime ().atZone (ZoneId.systemDefault ())
                    .toInstant().toEpochMilli ();
        }
    };

    @Autowired
    public void buildTopology (StreamsBuilder builder, KafkaProperties kafkaProperties) {

        SpecificAvroSerde<CreditCardTransaction> serializer = new SpecificAvroSerde<> ();
        var lockSerde = new SpecificAvroSerde<CreditCardLock> ();

        serializer.configure (kafkaProperties.buildConsumerProperties (null), false);
        lockSerde.configure (kafkaProperties.buildStreamsProperties (null), false);

        builder.stream (
                        "credit-card-transactions",
                        Consumed.with (Serdes.String (), serializer)
                                .withTimestampExtractor (extractor)
                )
                .filter ((key, transaction) -> transaction.getStatus () == TransactionStatus.REJECTED)
                .selectKey ((key, transaction) -> transaction.getCreaditCardNumber ())
                .repartition (Repartitioned.with (Serdes.String (), serializer)
                        .withName ("transactions.rejected.credit-card"))
                .groupByKey ()
                // create 5 minute window
                .windowedBy (
                        TimeWindows
                                .ofSizeAndGrace (Duration.ofMinutes (5), Duration.ofSeconds (30))
                                .advanceBy (Duration.ofMinutes (1)))
                .count (Materialized.as ("counts"))
                .toStream ()
                .filter ((windowedKey, countOfFailedTransactions) -> countOfFailedTransactions >= 5)
                .map ((windowedKey, count) ->
                        new KeyValue<> (
                                windowedKey.key (),
                                CreditCardLock.newBuilder ()
                                        .setCreditCardNumber (windowedKey.key ())
                                        .setDateTime (clock.now ())
                                        .setFailedTransactionsCount (count)
                                        .build ())
                )
                .peek ((creditCardNumber, lockDto) ->{
                    String threadName = Thread.currentThread ().getName ();
                            logger.info ("<-{}: {}", creditCardNumber, lockDto);
                            logger.info ("Thread: {}", threadName);
                        }

                )

                .to ("credit-card-lock", Produced.with (Serdes.String (), lockSerde));
/*                .foreach ((windowedKey, countOfFailedTransactions) -> logger.info (
                        "[{}] @ {}/{}: {}",
                        windowedKey.key (),
                        windowedKey.window ().startTime (),
                        windowedKey.window ().endTime (),
                        countOfFailedTransactions));*/
    }

}
