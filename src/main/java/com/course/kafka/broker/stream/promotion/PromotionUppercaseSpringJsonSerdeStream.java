package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
@Slf4j
public class PromotionUppercaseSpringJsonSerdeStream
{
    @Bean
    public KStream<String, PromotionMessage> kStreamPromotionUppercaseSpringJsonSerde(StreamsBuilder streamsBuilder)
    {
        JsonSerde<PromotionMessage> jsonSerde = new JsonSerde<>(PromotionMessage.class);
        KStream<String, PromotionMessage> sourceStream = streamsBuilder.stream("t.commodity.promotion",
                                                                               Consumed.with(Serdes.String(),
                                                                                             jsonSerde));

        KStream<String, PromotionMessage> uppercaseStream = sourceStream.mapValues(message -> new PromotionMessage(message.getPromotionCode()
                                                                                                                          .toUpperCase()));

        uppercaseStream.to("t.commodity.promotion-uppercase", Produced.with(Serdes.String(), jsonSerde));

        sourceStream.print(Printed.<String, PromotionMessage>toSysOut().withLabel("sourceStream"));
        uppercaseStream.print(Printed.<String, PromotionMessage>toSysOut().withLabel("uppercaseStream"));

        return sourceStream;
    }
}
