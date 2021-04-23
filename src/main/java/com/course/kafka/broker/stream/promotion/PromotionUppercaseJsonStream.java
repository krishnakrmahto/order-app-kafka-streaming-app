package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;

//@Configuration
@Slf4j
public class PromotionUppercaseJsonStream
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, String> kStreamPromotionUpperCaseJson(StreamsBuilder streamsBuilder)
    {
        Serde<String> stringSerde = Serdes.String();
        KStream<String, String> sourceStream = streamsBuilder.stream("t.commodity.promotion",
                                                                     Consumed.with(stringSerde, stringSerde));

        KStream<String, String> uppercaseStream = sourceStream.mapValues(this::uppercasePromotionCodeValue);
        uppercaseStream.to("t.commodity.promotion-uppercase");

        // useful for debugging, do not print in production
        sourceStream.print(Printed.<String, String>toSysOut().withLabel("JSON original stream"));
        uppercaseStream.print(Printed.<String, String>toSysOut().withLabel("JSON uppercase stream"));

        return sourceStream;
    }

    private String uppercasePromotionCodeValue(String message)
    {
        try {
            PromotionMessage promotionMessagePojo = objectMapper.readValue(message, PromotionMessage.class);
            return objectMapper.writeValueAsString(new PromotionMessage(promotionMessagePojo.getPromotionCode()
                                                                                            .toUpperCase()));
        }
        catch(JsonProcessingException e) {
            log.error("Unable to deserialize string to POJO");
            throw new RuntimeException("Error deserialize JSON string.", e);
        }
    }
}
