package com.avit.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ExchangeConfig {
    private static final Logger logger = LoggerFactory.getLogger(ExchangeConfig.class);
    private static final Properties properties = new Properties();
    private static ExchangeConfig instance;

    private ExchangeConfig() {
        loadProperties();
    }

    public static ExchangeConfig getInstance() {
        if (instance == null) {
            instance = new ExchangeConfig();
        }
        return instance;
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                logger.error("Unable to find application.properties");
                return;
            }
            properties.load(input);
        } catch (IOException e) {
            logger.error("Error loading properties file", e);
        }
    }

    public String getBinanceUrl() {
        return properties.getProperty("websocket.binance.url");
    }

    public String getOkxUrl() {
        return properties.getProperty("websocket.okx.url");
    }
}
