package com.avit.distruptor;

import com.avit.websocket.BinanceWebSocketClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.RingBuffer;
import java.util.List;

public class BinancePriceProducer extends AbstractPriceProducer {
    private BinanceWebSocketClient binanceWebSocketClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BinancePriceProducer(RingBuffer<PriceEvent> ringBuffer, String exchangeName, List<String> symbols) {
        super(ringBuffer, exchangeName, symbols);
        logger.info("BinancePriceProducer initialized with exchange name: {}", exchangeName);
    }

    @Override
    public void start() {
        try {
            logger.info("Starting BinancePriceProducer for symbols: {}", symbols);
            this.binanceWebSocketClient = new BinanceWebSocketClient(symbols, this::handleMessage);
            this.binanceWebSocketClient.start();
            logger.info("BinanceWebSocketClient started successfully");
        } catch (Exception e) {
            logger.error("Failed to start BinancePriceProducer", e);
        }
    }

    @Override
    protected void handleMessage(JsonNode jsonNode) {
        try {
            logger.debug("Received message: {}", jsonNode.toPrettyString());

            // Validate message structure
            if (!jsonNode.has("E") || !jsonNode.has("s") || !jsonNode.has("b") || !jsonNode.has("a")) {
                logger.warn("Message missing required fields: {}", jsonNode.toPrettyString());
                return;
            }

            long timestamp = jsonNode.get("E").asLong();
            String symbol = jsonNode.get("s").asText().toLowerCase(); // Convert to lowercase to match format

            List<List<String>> bids = objectMapper.convertValue(
                    jsonNode.get("b"),
                    new TypeReference<List<List<String>>>() {}
            );

            List<List<String>> asks = objectMapper.convertValue(
                    jsonNode.get("a"),
                    new TypeReference<List<List<String>>>() {}
            );

            logger.debug("Parsed message - Symbol: {}, Timestamp: {}, Bids size: {}, Asks size: {}",
                    symbol, timestamp, bids.size(), asks.size());

            if (bids.isEmpty() || asks.isEmpty()) {
                logger.warn("Empty bids or asks for symbol: {}", symbol);
                return;
            }

            try {
                double bid = Double.parseDouble(bids.get(0).get(0));
                double ask = Double.parseDouble(asks.get(0).get(0));

                logger.info("Publishing event - Exchange: {}, Symbol: {}, Bid: {}, Ask: {}", exchangeName, symbol, bid, ask);

                String keySymbol = symbol.equals("btcusdt") ? "BTC-USDT" : symbol;

                publishEvent(timestamp, keySymbol, bid, ask);

                logger.debug("Successfully published event to ring buffer");

            } catch (NumberFormatException e) {
                logger.error("Error parsing bid/ask values for symbol {}: bids={}, asks={}",
                        symbol, bids.get(0), asks.get(0), e);
            }

        } catch (Exception e) {
            logger.error("Error handling message: {}", jsonNode, e);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping BinancePriceProducer");
        if (this.binanceWebSocketClient != null) {
            this.binanceWebSocketClient.stop();
            logger.info("BinanceWebSocketClient stopped");
        }
    }
}