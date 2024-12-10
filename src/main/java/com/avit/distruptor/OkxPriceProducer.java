package com.avit.distruptor;

import com.avit.websocket.OkxWebSocketClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.lmax.disruptor.RingBuffer;
import java.util.List;

public class OkxPriceProducer extends AbstractPriceProducer {
    private OkxWebSocketClient okxWebSocketClient;

    public OkxPriceProducer(RingBuffer<PriceEvent> ringBuffer, String exchangeName, List<String> symbols) {
        super(ringBuffer, exchangeName, symbols);
    }

    @Override
    public void start() {
        try {
            this.okxWebSocketClient = new OkxWebSocketClient(symbols, this::handleMessage);
            this.okxWebSocketClient.start();
        } catch (Exception e) {
            logger.error("Failed to start OkxPriceProducer", e);
        }
    }

    @Override
    protected void handleMessage(JsonNode jsonNode) {
        try {
            JsonNode dataNode = jsonNode.path("data").get(0);
            if (dataNode == null) {
                logger.warn("No data found in the message.");
                return;
            }

            long timestamp = dataNode.path("ts").asLong();
            String symbol = dataNode.path("instId").asText();
            double buyLimit = dataNode.path("buyLmt").asDouble();
            double sellLimit = dataNode.path("sellLmt").asDouble();

            publishEvent(timestamp, symbol, buyLimit, sellLimit);

            logger.info("Okex - Published PriceEvent: {} Bid={}, Ask={}", symbol, buyLimit, sellLimit);
        } catch (Exception e) {
            logger.error("Error handling message: {}", jsonNode, e);
        }
    }

    @Override
    public void stop() {
        if (this.okxWebSocketClient != null) {
            this.okxWebSocketClient.stop();
        }
    }
}
