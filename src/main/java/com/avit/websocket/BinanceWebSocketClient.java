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
import java.util.stream.Collectors;


public class BinanceWebSocketClient {

    private static final Logger logger = LoggerFactory.getLogger(BinanceWebSocketClient.class);
    private final WebSocketClient webSocketClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String wsUrl = ExchangeConfig.getInstance().getBinanceUrl();


    public BinanceWebSocketClient(List<String> symbols, Consumer<JsonNode> messageHandler) throws Exception {
        String streamName = symbols.stream()
                .map(symbol -> symbol.toLowerCase() + "@depth@100ms")
                .collect(Collectors.joining("/"));

        String url = wsUrl +"/" + streamName;

        logger.info("Connecting to Binance WebSocket url: {}", url);

        this.webSocketClient = new WebSocketClient(new URI(url)) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                logger.info("Connected to Binance WebSocket for symbols: {}", symbols);
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
