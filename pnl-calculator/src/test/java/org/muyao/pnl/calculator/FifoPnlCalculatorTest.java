package org.muyao.pnl.calculator;

import org.junit.jupiter.api.Test;
import org.muyao.pnl.common.PnlSnapshot;
import org.muyao.pnl.common.Side;
import org.muyao.pnl.common.TransactionEvent;
import org.muyao.pnl.common.WindowDefinition;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FifoPnlCalculatorTest {
    @Test
    void computes_fifo_pnl_for_partial_sell() {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        List<TransactionEvent> events = List.of(
                tx("ACC-1", "AAPL", Side.BUY, "10", "100", base.plusSeconds(10)),
                tx("ACC-1", "AAPL", Side.BUY, "5", "110", base.plusSeconds(20)),
                tx("ACC-1", "AAPL", Side.SELL, "12", "120", base.plusSeconds(30))
        );

        PnlSnapshot snapshot = FifoPnlCalculator.computeWindowSnapshot(
                events,
                new WindowDefinition("1h", Duration.ofHours(1)),
                base.plusSeconds(30)
        );

        assertNotNull(snapshot);
        assertEquals(decimal("3.00000000"), snapshot.getPositionQuantity());
        assertEquals(decimal("110.00000000"), snapshot.getAverageCost());
        assertEquals(decimal("260.00000000"), snapshot.getRealizedPnl());
        assertEquals(decimal("30.00000000"), snapshot.getUnrealizedPnl());
        assertEquals(decimal("290.00000000"), snapshot.getTotalPnl());
    }

    @Test
    void clamps_sell_quantity_to_available_inventory() {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        List<TransactionEvent> events = List.of(
                tx("ACC-1", "AAPL", Side.BUY, "2", "100", base.plusSeconds(10)),
                tx("ACC-1", "AAPL", Side.SELL, "5", "130", base.plusSeconds(20))
        );

        PnlSnapshot snapshot = FifoPnlCalculator.computeWindowSnapshot(
                events,
                new WindowDefinition("1h", Duration.ofHours(1)),
                base.plusSeconds(20)
        );

        assertNotNull(snapshot);
        assertEquals(decimal("0.00000000"), snapshot.getPositionQuantity());
        assertEquals(decimal("0.00000000"), snapshot.getAverageCost());
        assertEquals(decimal("60.00000000"), snapshot.getRealizedPnl());
        assertEquals(decimal("0.00000000"), snapshot.getUnrealizedPnl());
        assertEquals(decimal("60.00000000"), snapshot.getTotalPnl());
    }

    @Test
    void filters_events_outside_requested_window() {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        List<TransactionEvent> events = List.of(
                tx("ACC-1", "AAPL", Side.BUY, "10", "100", base.minus(Duration.ofHours(2))),
                tx("ACC-1", "AAPL", Side.BUY, "4", "105", base.minus(Duration.ofMinutes(30))),
                tx("ACC-1", "AAPL", Side.SELL, "1", "120", base)
        );

        PnlSnapshot snapshot = FifoPnlCalculator.computeWindowSnapshot(
                events,
                new WindowDefinition("1h", Duration.ofHours(1)),
                base
        );

        assertNotNull(snapshot);
        assertEquals(decimal("3.00000000"), snapshot.getPositionQuantity());
        assertEquals(decimal("105.00000000"), snapshot.getAverageCost());
        assertEquals(decimal("15.00000000"), snapshot.getRealizedPnl());
        assertEquals(decimal("45.00000000"), snapshot.getUnrealizedPnl());
        assertEquals(decimal("60.00000000"), snapshot.getTotalPnl());
    }

    @Test
    void returns_null_when_no_events_fall_inside_window() {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        List<TransactionEvent> events = List.of(
                tx("ACC-1", "AAPL", Side.BUY, "1", "100", base.minus(Duration.ofDays(2)))
        );

        PnlSnapshot snapshot = FifoPnlCalculator.computeWindowSnapshot(
                events,
                new WindowDefinition("1h", Duration.ofHours(1)),
                base
        );

        assertNull(snapshot);
    }

    private static TransactionEvent tx(String account, String symbol, Side side, String quantity, String price, Instant eventTime) {
        TransactionEvent event = new TransactionEvent();
        event.setTradeId(account + "-" + symbol + "-" + eventTime);
        event.setAccountId(account);
        event.setSymbol(symbol);
        event.setSide(side);
        event.setQuantity(decimal(quantity));
        event.setPrice(decimal(price));
        event.setEventTime(eventTime);
        return event;
    }

    private static BigDecimal decimal(String value) {
        return new BigDecimal(value).setScale(FifoPnlCalculator.SCALE);
    }
}
