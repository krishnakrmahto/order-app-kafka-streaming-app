package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
@Slf4j
public class MaskOrderStream
{
    @Bean
    public KStream<String, OrderMessage> kStreamMaskOrder(StreamsBuilder streamsBuilder)
    {
        JsonSerde<OrderMessage> orderMessageJsonSerde = new JsonSerde<>(OrderMessage.class);

        KStream<String, OrderMessage> orderMessageStream = streamsBuilder.stream("t.commodity.order",
                                                                                 Consumed.with(Serdes.String(), orderMessageJsonSerde));
        orderMessageStream.mapValues(Util::maskCreditCardNumber)
                          .to("t.commodity.order-masked", Produced.with(Serdes.String(), orderMessageJsonSerde));

        return orderMessageStream;
    }
}
