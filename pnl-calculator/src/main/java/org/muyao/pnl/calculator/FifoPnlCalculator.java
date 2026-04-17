package org.muyao.pnl.calculator;

import org.muyao.pnl.common.PnlSnapshot;
import org.muyao.pnl.common.Side;
import org.muyao.pnl.common.TransactionEvent;
import org.muyao.pnl.common.WindowDefinition;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public final class FifoPnlCalculator {
    public static final int SCALE = 8;

    private FifoPnlCalculator() {
    }

    public static BigDecimal scale(BigDecimal value) {
        return value.setScale(SCALE, RoundingMode.HALF_UP);
    }

    public static PnlSnapshot computeWindowSnapshot(List<TransactionEvent> allEvents, WindowDefinition window, Instant asOf) {
        Instant cutoff = asOf.minus(window.duration());
        List<TransactionEvent> filtered = allEvents.stream()
                .filter(event -> event.getEventTime() != null && !event.getEventTime().isBefore(cutoff) && !event.getEventTime().isAfter(asOf))
                .sorted(Comparator.comparing(TransactionEvent::getEventTime))
                .toList();

        if (filtered.isEmpty()) {
            return null;
        }

        Deque<Lot> lots = new ArrayDeque<>();
        BigDecimal lastPrice = BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);
        BigDecimal realizedPnl = BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);

        for (TransactionEvent event : filtered) {
            BigDecimal quantity = scale(event.getQuantity());
            BigDecimal price = scale(event.getPrice());
            lastPrice = price;

            if (event.getSide() == Side.BUY) {
                lots.addLast(new Lot(quantity, price));
                continue;
            }

            BigDecimal remainingToSell = quantity;
            while (remainingToSell.compareTo(BigDecimal.ZERO) > 0 && !lots.isEmpty()) {
                Lot lot = lots.peekFirst();
                BigDecimal matched = remainingToSell.min(lot.remainingQuantity());
                realizedPnl = scale(realizedPnl.add(price.subtract(lot.price()).multiply(matched)));
                BigDecimal newRemaining = scale(lot.remainingQuantity().subtract(matched));
                remainingToSell = scale(remainingToSell.subtract(matched));
                if (newRemaining.compareTo(BigDecimal.ZERO) <= 0) {
                    lots.removeFirst();
                } else {
                    lots.removeFirst();
                    lots.addFirst(new Lot(newRemaining, lot.price()));
                }
            }
        }

        BigDecimal positionQuantity = BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);
        BigDecimal totalCost = BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);
        BigDecimal unrealizedPnl = BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);
        for (Lot lot : lots) {
            positionQuantity = scale(positionQuantity.add(lot.remainingQuantity()));
            totalCost = scale(totalCost.add(lot.remainingQuantity().multiply(lot.price())));
            unrealizedPnl = scale(unrealizedPnl.add(lastPrice.subtract(lot.price()).multiply(lot.remainingQuantity())));
        }

        BigDecimal averageCost = positionQuantity.compareTo(BigDecimal.ZERO) == 0
                ? BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP)
                : scale(totalCost.divide(positionQuantity, SCALE, RoundingMode.HALF_UP));

        TransactionEvent latest = filtered.get(filtered.size() - 1);
        PnlSnapshot snapshot = new PnlSnapshot();
        snapshot.setAccountId(latest.getAccountId());
        snapshot.setSymbol(latest.getSymbol());
        snapshot.setWindowName(window.name());
        snapshot.setPositionQuantity(positionQuantity);
        snapshot.setAverageCost(averageCost);
        snapshot.setLastPrice(lastPrice);
        snapshot.setRealizedPnl(realizedPnl);
        snapshot.setUnrealizedPnl(unrealizedPnl);
        snapshot.setTotalPnl(scale(realizedPnl.add(unrealizedPnl)));
        snapshot.setUpdatedAt(asOf);
        return snapshot;
    }

    public record Lot(BigDecimal remainingQuantity, BigDecimal price) {
    }
}
