package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class InventoryStreamFour
{
    @Bean
    public KStream<String, InventoryMessage> kStreamInventoryThree(StreamsBuilder streamsBuilder,
                                                                   InventoryTimestampExtractor timestampExtractor)
    {
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        JsonSerde<InventoryMessage> inventoryMessageSerde = new JsonSerde<>(InventoryMessage.class);

        KStream<String, InventoryMessage> sourceStream = streamsBuilder.stream("t.commodity.inventory",
                                                                               Consumed.with(stringSerde,
                                                                                             inventoryMessageSerde,
                                                                                             timestampExtractor,
                                                                                             null));

        sourceStream.to("t.commodity.inventory-total-four", Produced.with(stringSerde, inventoryMessageSerde));

        return sourceStream;
    }
}
