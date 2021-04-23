package com.course.kafka.broker.stream.promotion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;

//@Configuration
public class PromotionUppercaseStream
{
    @Bean
    public KStream<String, String> kStreamPromotionUppercase(StreamsBuilder streamsBuilder)
    {
        // the next 3 lines create a topology for transforming promotion names to uppercase, creating stream (arrow in DAG)
        // from source topic and sink topic with stream processor (node in DAG) in between.
        KStream<String, String> sourceStream = streamsBuilder.stream("t.commodity.promotion", Consumed.with(Serdes.String(),
                                                                                                     Serdes.String()));

        KStream<String, String> uppercaseStream = sourceStream.mapValues((ValueMapper<String, String>) String::toUpperCase);

        uppercaseStream.to("t.commodity.promotion-uppercase", Produced.with(Serdes.String(), Serdes.String()));

        // useful for debugging but don't do the following on production
        sourceStream.print(Printed.<String, String>toSysOut().withLabel("sourceStream"));
        uppercaseStream.print(Printed.<String, String>toSysOut().withLabel("uppercaseStream"));

        return sourceStream;
    }
}
