package com.avit.distruptor;

import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PriceEventHandler implements EventHandler<PriceEvent> {

    private final static double MIN_PROFIT = 500;
    private static final Logger logger = LoggerFactory.getLogger(PriceEventHandler.class);
    private final Map<String, Map<String, PriceEvent>> latestPrices = new ConcurrentHashMap<>();

    @Override
    public void onEvent(PriceEvent event, long sequence, boolean endOfBatch) throws Exception {

        latestPrices.computeIfAbsent(event.getSymbol(), k -> new ConcurrentHashMap<>())
                .put(event.getExchange(), event);

        Map<String, PriceEvent> symbolPrices = latestPrices.get(event.getSymbol());

        logger.info("Received event - Sequence: {}, Exchange: {}, Symbol: {}, Bid: {}, Ask: {}",
                sequence, event.getExchange(), event.getSymbol(), event.getBid(), event.getAsk());

        if (symbolPrices.size() > 1) {

            PriceEvent binancePrice = symbolPrices.get("Binance");
            PriceEvent okexPrice = symbolPrices.get("Okex");

            if (binancePrice != null && okexPrice != null) {
                // Check Binance -> Okex arbitrage
                if (binancePrice.getAsk() < okexPrice.getBid()) {
                    double profit = okexPrice.getBid() - binancePrice.getAsk();
                    if(profit > MIN_PROFIT) {
                        logger.info("Arbitrage opportunity for {}: Buy from Binance ({}) and sell on Okex ({}), Profit: {}",
                                event.getSymbol(), binancePrice.getAsk(), okexPrice.getBid(), profit);

                    }
                }

                // Check Okex -> Binance arbitrage
                if (okexPrice.getAsk() < binancePrice.getBid()) {
                    double profit = binancePrice.getBid() - okexPrice.getAsk();
                    if(profit > MIN_PROFIT) {
                        logger.info("Arbitrage opportunity for {}: Buy from Okex ({}) and sell on Binance ({}), Profit: {}",
                                event.getSymbol(), okexPrice.getAsk(), binancePrice.getBid(), profit);

                    }
                }
            }
        }
    }
}