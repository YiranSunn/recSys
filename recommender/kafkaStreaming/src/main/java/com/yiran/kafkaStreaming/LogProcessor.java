package com.yiran.kafkaStreaming;

import org.apache.kafka.streams.processor.Processor;

import org.apache.kafka.streams.processor.ProcessorContext;





public class LogProcessor implements Processor<byte[],byte[]> {


    private ProcessorContext context;

    @Override

    public void init(ProcessorContext processorContext) {

        this.context = processorContext;

    }

    //core

    @Override
    public void process(byte[] dummy, byte[] line) {

        String input = new String(line);

        if(input.contains("PRODUCT_RATING_PREFIX:")){

            System.out.println("product rating coming!" + input);

            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();

            context.forward("logProcessor".getBytes(), input.getBytes());

        }

    }

    @Override
    public void punctuate(long timestamp) {


    }

    public void close() {


    }

}
