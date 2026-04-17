package org.muyao.pnl.common;

import java.time.Duration;
import java.util.List;

public record WindowDefinition(String name, Duration duration) {
    public static List<WindowDefinition> supported() {
        return List.of(
                new WindowDefinition("1m", Duration.ofMinutes(1)),
                new WindowDefinition("5m", Duration.ofMinutes(5)),
                new WindowDefinition("30m", Duration.ofMinutes(30)),
                new WindowDefinition("1h", Duration.ofHours(1)),
                new WindowDefinition("6h", Duration.ofHours(6)),
                new WindowDefinition("12h", Duration.ofHours(12)),
                new WindowDefinition("1d", Duration.ofDays(1)),
                new WindowDefinition("1w", Duration.ofDays(7)),
                new WindowDefinition("1mo", Duration.ofDays(30)),
                new WindowDefinition("6mo", Duration.ofDays(180)),
                new WindowDefinition("1y", Duration.ofDays(365)),
                new WindowDefinition("5y", Duration.ofDays(365 * 5L)),
                new WindowDefinition("10y", Duration.ofDays(365 * 10L))
        );
    }
}
