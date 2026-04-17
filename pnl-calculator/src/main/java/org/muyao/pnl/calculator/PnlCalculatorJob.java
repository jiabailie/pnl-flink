package org.muyao.pnl.calculator;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.muyao.pnl.common.JsonSerde;
import org.muyao.pnl.common.PnlSnapshot;
import org.muyao.pnl.common.Side;
import org.muyao.pnl.common.TransactionEvent;
import org.muyao.pnl.common.WindowDefinition;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PnlCalculatorJob {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String topic = env("KAFKA_TOPIC", "transactions");
        String groupId = env("KAFKA_GROUP_ID", "pnl-calculator");
        String postgresUrl = env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/pnl");
        String postgresUser = env("POSTGRES_USER", "pnl");
        String postgresPassword = env("POSTGRES_PASSWORD", "pnl");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (event, previousTimestamp) -> {
                    try {
                        TransactionEvent transaction = JsonSerde.mapper().readValue(event, TransactionEvent.class);
                        return transaction.getEventTime() == null ? System.currentTimeMillis() : transaction.getEventTime().toEpochMilli();
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                });

        DataStream<TransactionEvent> transactions = env.fromSource(source, watermarkStrategy, "kafka-transactions")
                .flatMap(new TransactionParser())
                .returns(Types.POJO(TransactionEvent.class));

        DataStream<PnlSnapshot> snapshots = transactions
                .keyBy(tx -> tx.getAccountId() + ":" + tx.getSymbol())
                .process(new FifoPnlProcessFunction());

        snapshots.addSink(JdbcSink.sink(
                """
                insert into pnl_snapshots (
                    account_id,
                    symbol,
                    window_name,
                    position_qty,
                    avg_cost,
                    last_price,
                    realized_pnl,
                    unrealized_pnl,
                    total_pnl,
                    updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict (account_id, symbol, window_name) do update set
                    position_qty = excluded.position_qty,
                    avg_cost = excluded.avg_cost,
                    last_price = excluded.last_price,
                    realized_pnl = excluded.realized_pnl,
                    unrealized_pnl = excluded.unrealized_pnl,
                    total_pnl = excluded.total_pnl,
                    updated_at = excluded.updated_at
                """,
                (PreparedStatement statement, PnlSnapshot snapshot) -> {
                    statement.setString(1, snapshot.getAccountId());
                    statement.setString(2, snapshot.getSymbol());
                    statement.setString(3, snapshot.getWindowName());
                    statement.setBigDecimal(4, snapshot.getPositionQuantity());
                    statement.setBigDecimal(5, snapshot.getAverageCost());
                    statement.setBigDecimal(6, snapshot.getLastPrice());
                    statement.setBigDecimal(7, snapshot.getRealizedPnl());
                    statement.setBigDecimal(8, snapshot.getUnrealizedPnl());
                    statement.setBigDecimal(9, snapshot.getTotalPnl());
                    statement.setTimestamp(10, Timestamp.from(snapshot.getUpdatedAt()));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .withBatchIntervalMs(0)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(postgresUser)
                        .withPassword(postgresPassword)
                        .build()
        )).name("postgres-upsert-sink");

        env.execute("fifo-pnl-calculator");
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    public static final class TransactionParser implements FlatMapFunction<String, TransactionEvent> {
        @Override
        public void flatMap(String value, Collector<TransactionEvent> out) throws Exception {
            try {
                out.collect(JsonSerde.mapper().readValue(value, TransactionEvent.class));
            } catch (Exception ignored) {
            }
        }
    }

    public static final class FifoPnlProcessFunction extends KeyedProcessFunction<String, TransactionEvent, PnlSnapshot> {
        private transient ListState<TransactionEvent> eventsState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            eventsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("transaction-events", Types.POJO(TransactionEvent.class))
            );
        }

        @Override
        public void processElement(TransactionEvent tx, Context context, Collector<PnlSnapshot> out) throws Exception {
            List<TransactionEvent> events = new ArrayList<>();
            for (TransactionEvent existing : eventsState.get()) {
                events.add(existing);
            }
            events.add(tx);
            events.sort(Comparator.comparing(TransactionEvent::getEventTime, Comparator.nullsLast(Comparator.naturalOrder())));
            eventsState.update(events);

            Instant asOf = tx.getEventTime() == null ? Instant.now() : tx.getEventTime();
            for (WindowDefinition window : WindowDefinition.supported()) {
                PnlSnapshot snapshot = FifoPnlCalculator.computeWindowSnapshot(events, window, asOf);
                if (snapshot != null) {
                    out.collect(snapshot);
                }
            }
        }
    }
}
