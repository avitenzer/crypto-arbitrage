package com.avit.websocket;

import com.avit.config.ExchangeConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.function.Consumer;

public class OkxWebSocketClient {

    private static final Logger logger = LoggerFactory.getLogger(BinanceWebSocketClient.class);
    private final WebSocketClient webSocketClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String url = ExchangeConfig.getInstance().getOkxUrl();

    public OkxWebSocketClient(List<String> symbols, Consumer<JsonNode> messageHandler) throws Exception {

        logger.info("Connecting to Binance WebSocket url: {}", url);

        this.webSocketClient = new WebSocketClient(new URI(url)) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                for (String symbol : symbols) {
                    logger.info("Connected to Binance WebSocket for symbols: {}", symbol);
                    String subscriptionMessage = "{\"op\": \"subscribe\", \"args\": [{\"channel\": \"price-limit\", \"instId\": \""+symbol+"\"}]}";
                    send(subscriptionMessage);
                }
            }

            @Override
            public void onMessage(String message) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(message);
                    messageHandler.accept(jsonNode);
                } catch (Exception e) {
                    logger.error("Error processing message: {}", message, e);
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                logger.info("Disconnected from Binance WebSocket: {}", reason);
            }

            @Override
            public void onError(Exception ex) {
                logger.error("WebSocket error: ", ex);
            }
        };
    }

    public void start() {
        this.webSocketClient.connect();
    }

    public void stop() {
        this.webSocketClient.close();
    }

}
