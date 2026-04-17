package org.muyao.pnl.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.muyao.pnl.common.JsonSerde;
import org.muyao.pnl.common.Side;
import org.muyao.pnl.common.TransactionEvent;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionGeneratorApp {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "transactions");
        int accountCount = Integer.parseInt(env("GENERATOR_ACCOUNT_COUNT", "5"));
        long intervalMs = Long.parseLong(env("GENERATOR_INTERVAL_MS", "1000"));
        List<String> symbols = Arrays.stream(env("GENERATOR_SYMBOLS", "AAPL,MSFT,GOOG,TSLA,NVDA").split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                TransactionEvent transaction = randomTransaction(accountCount, symbols);
                String payload = JsonSerde.write(transaction);
                String key = transaction.getAccountId() + ":" + transaction.getSymbol();
                producer.send(new ProducerRecord<>(topic, key, payload)).get();
                System.out.println(payload);
                Thread.sleep(intervalMs);
            }
        }
    }

    private static TransactionEvent randomTransaction(int accountCount, List<String> symbols) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TransactionEvent transaction = new TransactionEvent();
        transaction.setTradeId(UUID.randomUUID().toString());
        transaction.setAccountId("ACC-" + (random.nextInt(accountCount) + 1));
        transaction.setSymbol(symbols.get(random.nextInt(symbols.size())));
        transaction.setSide(random.nextBoolean() ? Side.BUY : Side.SELL);
        transaction.setQuantity(decimal(random.nextDouble(1.0, 50.0)));
        transaction.setPrice(decimal(random.nextDouble(50.0, 500.0)));
        transaction.setEventTime(Instant.now());
        return transaction;
    }

    private static BigDecimal decimal(double value) {
        return BigDecimal.valueOf(value).setScale(8, RoundingMode.HALF_UP);
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
