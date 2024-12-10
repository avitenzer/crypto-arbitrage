package com.avit;

import com.avit.distruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ArbitrageApp {
    private static final Logger logger = LoggerFactory.getLogger(ArbitrageApp.class);
    private final List<AbstractPriceProducer> producers = new ArrayList<>();
    private final ExecutorService executor;
    private final Disruptor<PriceEvent> disruptor;

    public ArbitrageApp() {
        this.executor = Executors.newCachedThreadPool();
        PriceEventFactory factory = new PriceEventFactory();
        int bufferSize = 1024;
        this.disruptor = new Disruptor<>(factory, bufferSize, executor);
        this.disruptor.handleEventsWith(new PriceEventHandler());
    }

    public void addProducer(AbstractPriceProducer producer) {
        producers.add(producer);
    }

    public void start() {
        logger.info("Starting Disruptor Application...");
        disruptor.start();

        for (AbstractPriceProducer producer : producers) {
            producer.start();
            logger.info("{} started. Listening for price events...", producer.getClass().getSimpleName());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public void shutdown() {
        logger.info("Shutting down application...");

        for (AbstractPriceProducer producer : producers) {
            logger.info("Shutting down {}...", producer.getClass().getSimpleName());
            producer.stop();
        }

        disruptor.shutdown();
        executor.shutdown();
    }

    public static void main(String[] args) {
        ArbitrageApp app = new ArbitrageApp();

        List<String> binanceSymbols = Arrays.asList("btcusdt");
        AbstractPriceProducer binanceProducer = new BinancePriceProducer(
                app.disruptor.getRingBuffer(),
                "Binance",
                binanceSymbols
        );

        List<String> okxSymbols = Arrays.asList("BTC-USDT");
        AbstractPriceProducer okxProducer = new OkxPriceProducer(
                app.disruptor.getRingBuffer(),
                "Okex",
                okxSymbols
        );

        app.addProducer(binanceProducer);
        app.addProducer(okxProducer);

        app.start();
    }
}