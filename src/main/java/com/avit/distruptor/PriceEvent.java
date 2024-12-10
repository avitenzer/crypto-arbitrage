package com.avit.distruptor;


import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class PriceEvent {

    private String exchange;
    private String symbol;
    private double bid;
    private double ask;
    private long timestamp;
}
