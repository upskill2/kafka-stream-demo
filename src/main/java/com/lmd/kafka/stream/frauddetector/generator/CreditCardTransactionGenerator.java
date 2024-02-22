package com.lmd.kafka.stream.frauddetector.generator;

import com.lmd.kafka.stream.frauddetector.CreditCardTransaction;
import com.lmd.kafka.stream.frauddetector.TransactionStatus;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

@Component
public class CreditCardTransactionGenerator {

    private Faker faker = new Faker ();
    private Random random = new Random ();

    @Autowired
    private KafkaTemplate<String, CreditCardTransaction> kafkaTemplate;

    @Scheduled (fixedDelay = 500)
    public void generateTransaction () {
        CustomerInfo customer = buildCustomerInfo ().get (random.nextInt (2));

        CreditCardTransaction transaction = CreditCardTransaction.newBuilder ()
                .setTransactionId (UUID.randomUUID ().toString ())
                .setDateTime (LocalDateTime.now ())
                .setCustomerId (customer.id ())
                .setCreaditCardNumber (customer.creditCardId ())
                .setCreaditCardExpiry (customer.cardExpire ())
                .setStatus (random.nextBoolean () ? TransactionStatus.ACCEPTED : TransactionStatus.REJECTED)
                .setAmount (faker.number ().randomDouble (2, 100, 300))
                .build ();

        kafkaTemplate.send ("credit-card-transactions", transaction.getTransactionId (), transaction);

    }

    private List<CustomerInfo> buildCustomerInfo () {
        List<String> creditCardsNumbers = List.of (
                "4716-2210-5188-5662",
                "4716-2210-5188-5663",
                "4716-2210-5188-5664",
                "4716-2210-5188-5665",
                "4716-2210-5188-5666",
                "4716-2210-5188-5667",
                "4716-2210-5188-5668",
                "4716-2210-5188-5669",
                "4716-2210-5188-5670",
                "4716-2210-5188-5671"
        );
        return Stream.generate (
                        () -> new CustomerInfo (
                                faker.idNumber ().valid (),
                                creditCardsNumbers.get (random.nextInt (creditCardsNumbers.size ())),
                                faker.business ().creditCardExpiry ())
                ).limit (50)
                .toList ();
    }

}
