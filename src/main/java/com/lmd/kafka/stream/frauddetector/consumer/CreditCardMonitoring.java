package com.lmd.kafka.stream.frauddetector.consumer;

import com.lmd.kafka.stream.frauddetector.CreditCardLock;
import com.lmd.kafka.stream.frauddetector.CreditCardTransaction;
import com.lmd.kafka.stream.frauddetector.TransactionStatus;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class CreditCardMonitoring {

    Logger logger = org.apache.logging.log4j.LogManager.getLogger (CreditCardMonitoring.class);

    @Autowired
    public void buildTopology (StreamsBuilder builder, KafkaProperties kafkaProperties) {

        SpecificAvroSerde<CreditCardTransaction> serializer = new SpecificAvroSerde<CreditCardTransaction> ();
        serializer.configure (kafkaProperties.buildConsumerProperties (null), false);

        builder.stream (
                        "credit-card-transactions",
                        Consumed.with (Serdes.String (), serializer)
                )
                .filter ((key, transaction) -> transaction.getStatus () == TransactionStatus.REJECTED)
                .selectKey ((key, transaction) -> transaction.getCreaditCardNumber ())
                .repartition (Repartitioned.with (Serdes.String (), serializer)
                        .withName ("transactions.rejected.credit-card"))
                .groupByKey ()
                // create 5 minute window
                .windowedBy (TimeWindows.ofSizeWithNoGrace (Duration.ofMinutes (5)))

                .count (Materialized.as ("counts"))
                .toStream ()
                .filter ((windowedKey, countOfFailedTransactions) -> countOfFailedTransactions > 5)
                .map ((windowedKey, count) -> new KeyValue<> (
                        windowedKey,
                        new CreditCardLock.Builder ()
                                .setCreditCardNumber (windowedKey.key ()
                                ).build ())
                        .foreach ((windowedKey, countOfFailedTransactions) -> logger.info (
                                "[{}] @ {}/{}: {}",
                                windowedKey.key (),
                                windowedKey.window ().startTime (),
                                windowedKey.window ().endTime (),
                                countOfFailedTransactions));
    }

}
