package com.yiran.kafkaStreaming;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.processor.TopologyBuilder;

import org.apache.kafka.streams.KafkaStreams;

public class Application {


    public static void main(String[] args){


        String brokers = "localhost:9092";

        String zookeepers = "localhost:2181";

        String from = "log";

        String to = "recommender";

        //kafka stream config

        Properties settings = new Properties();

        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");

        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        //kafka stream config object

        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from).addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE").addSink("SINK", to, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();

        System.out.println("kafka stream start");


    }



}
