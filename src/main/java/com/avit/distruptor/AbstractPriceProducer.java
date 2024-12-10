package com.avit.distruptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractPriceProducer {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractPriceProducer.class);
    protected final RingBuffer<PriceEvent> ringBuffer;
    protected final String exchangeName;
    protected final List<String> symbols;

    protected AbstractPriceProducer(RingBuffer<PriceEvent> ringBuffer, String exchangeName, List<String> symbols) {
        this.ringBuffer = ringBuffer;
        this.exchangeName = exchangeName;
        this.symbols = symbols;
    }

    public abstract void start();
    public abstract void stop();
    protected abstract void handleMessage(JsonNode jsonNode);

    protected void publishEvent(long timestamp, String symbol, double bid, double ask) {
        long sequence = ringBuffer.next();
        try {
            PriceEvent event = ringBuffer.get(sequence);
            event.setTimestamp(timestamp);
            event.setExchange(exchangeName);
            event.setSymbol(symbol);
            event.setBid(bid);
            event.setAsk(ask);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}