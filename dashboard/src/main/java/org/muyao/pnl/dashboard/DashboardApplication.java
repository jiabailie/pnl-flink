package org.muyao.pnl.dashboard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

public class DashboardApplication {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(env("DASHBOARD_PORT", "8088"));
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", DashboardApplication::handleIndex);
        server.start();
        System.out.println("Dashboard listening on http://localhost:" + port);
    }

    private static void handleIndex(HttpExchange exchange) throws IOException {
        try {
            Map<String, String> filters = parseQuery(exchange.getRequestURI());
            String html = renderPage(filters);
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        } catch (Exception exception) {
            byte[] bytes = ("<h1>Dashboard error</h1><pre>" + escapeHtml(exception.getMessage()) + "</pre>").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(500, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }
    }

    private static String renderPage(Map<String, String> filters) throws Exception {
        String account = filters.getOrDefault("account", "");
        String symbol = filters.getOrDefault("symbol", "");
        String window = filters.getOrDefault("window", "");

        StringBuilder rows = new StringBuilder();
        try (Connection connection = DriverManager.getConnection(
                env("POSTGRES_URL", "jdbc:postgresql://localhost:5432/pnl"),
                env("POSTGRES_USER", "pnl"),
                env("POSTGRES_PASSWORD", "pnl"));
             Statement statement = connection.createStatement()) {

            StringBuilder sql = new StringBuilder("""
                    select account_id, symbol, window_name, position_qty, avg_cost, last_price,
                           realized_pnl, unrealized_pnl, total_pnl, updated_at
                    from pnl_snapshots
                    where 1=1
                    """);
            if (!account.isBlank()) {
                sql.append(" and account_id = '").append(account.replace("'", "''")).append("'");
            }
            if (!symbol.isBlank()) {
                sql.append(" and symbol = '").append(symbol.replace("'", "''")).append("'");
            }
            if (!window.isBlank()) {
                sql.append(" and window_name = '").append(window.replace("'", "''")).append("'");
            }
            sql.append(" order by account_id, symbol, window_name limit 500");

            try (ResultSet resultSet = statement.executeQuery(sql.toString())) {
                while (resultSet.next()) {
                    rows.append("<tr>")
                            .append(td(resultSet.getString("account_id")))
                            .append(td(resultSet.getString("symbol")))
                            .append(td(resultSet.getString("window_name")))
                            .append(td(resultSet.getString("position_qty")))
                            .append(td(resultSet.getString("avg_cost")))
                            .append(td(resultSet.getString("last_price")))
                            .append(td(resultSet.getString("realized_pnl")))
                            .append(td(resultSet.getString("unrealized_pnl")))
                            .append(td(resultSet.getString("total_pnl")))
                            .append(td(resultSet.getString("updated_at")))
                            .append("</tr>");
                }
            }
        }

        return """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                  <meta charset="UTF-8">
                  <title>FIFO PnL Dashboard</title>
                  <style>
                    body { font-family: Georgia, serif; margin: 24px; background: linear-gradient(180deg, #f4f1ea, #ffffff); color: #222; }
                    h1 { margin-bottom: 8px; }
                    form { display: flex; gap: 12px; flex-wrap: wrap; margin: 16px 0 24px; }
                    input { padding: 8px 10px; border: 1px solid #b9b1a3; background: #fffdf8; }
                    button { padding: 8px 14px; border: 0; background: #1f4b3f; color: white; cursor: pointer; }
                    table { width: 100%; border-collapse: collapse; background: white; }
                    th, td { border-bottom: 1px solid #ddd6ca; padding: 10px; text-align: left; font-size: 14px; }
                    th { background: #f0e8d9; }
                  </style>
                </head>
                <body>
                  <h1>FIFO PnL Dashboard</h1>
                  <p>Current snapshot view from PostgreSQL.</p>
                  <form method="get">
                    <input type="text" name="account" placeholder="Account" value="%s">
                    <input type="text" name="symbol" placeholder="Symbol" value="%s">
                    <input type="text" name="window" placeholder="Window" value="%s">
                    <button type="submit">Filter</button>
                  </form>
                  <table>
                    <thead>
                      <tr>
                        <th>Account</th><th>Symbol</th><th>Window</th><th>Position</th><th>Avg Cost</th>
                        <th>Last Price</th><th>Realized</th><th>Unrealized</th><th>Total</th><th>Updated</th>
                      </tr>
                    </thead>
                    <tbody>%s</tbody>
                  </table>
                </body>
                </html>
                """.formatted(
                escapeHtml(account),
                escapeHtml(symbol),
                escapeHtml(window),
                rows
        );
    }

    private static Map<String, String> parseQuery(URI uri) {
        Map<String, String> values = new LinkedHashMap<>();
        String query = uri.getRawQuery();
        if (query == null || query.isBlank()) {
            return values;
        }
        for (String part : query.split("&")) {
            int separator = part.indexOf('=');
            if (separator > 0) {
                values.put(part.substring(0, separator), part.substring(separator + 1));
            }
        }
        return values;
    }

    private static String td(String value) {
        return "<td>" + escapeHtml(value) + "</td>";
    }

    private static String escapeHtml(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
