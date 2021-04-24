package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import com.course.kafka.broker.serde.CustomJsonDeserializer;
import com.course.kafka.broker.serde.CustomJsonSerde;
import com.course.kafka.broker.serde.CustomJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;

//@Configuration
@Slf4j
public class PromotionUpperCaseCustomJsonSerdeStream
{
    @Bean
    public KStream<String, PromotionMessage> kStreamPromotionUpperCaseCustomJsonSerde(StreamsBuilder streamsBuilder)
    {
        CustomJsonSerde<PromotionMessage> customJsonSerde = new CustomJsonSerde<>(new CustomJsonSerializer<>(),
                                                                            new CustomJsonDeserializer<>(PromotionMessage.class));

        KStream<String, PromotionMessage> sourceStream = streamsBuilder.stream("t.commodity.promotion",
                                                                               Consumed.with(Serdes.String(), customJsonSerde));

        KStream<String, PromotionMessage> upperCaseStream = sourceStream.mapValues(message -> new PromotionMessage(message.getPromotionCode()
                                                                                                                          .toUpperCase()));

        upperCaseStream.to("t.commodity.promotion-uppercase", Produced.with(Serdes.String(), customJsonSerde));

        sourceStream.print(Printed.<String, PromotionMessage>toSysOut().withLabel("sourceStream"));
        upperCaseStream.print(Printed.<String, PromotionMessage>toSysOut().withLabel("upperCaseStream"));

        return sourceStream;
    }
}
