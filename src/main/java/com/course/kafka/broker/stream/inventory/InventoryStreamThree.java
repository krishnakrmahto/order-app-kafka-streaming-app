package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class InventoryStreamThree
{
    @Bean
    public KStream<String, InventoryMessage> kStreamInventoryThree(StreamsBuilder streamsBuilder)
    {
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, InventoryMessage> sourceStream = streamsBuilder.stream("t.commodity.inventory",
                                                                               Consumed.with(stringSerde, new JsonSerde<>(InventoryMessage.class)));

        sourceStream.mapValues(value -> "ADD".equals(value.getType()) ? value.getQuantity() : -value.getQuantity())
                    .groupByKey()
                    .reduce(Long::sum, Materialized.with(stringSerde, longSerde))
                    .toStream().to("t.commodity.inventory-total-three", Produced.with(stringSerde, longSerde));

        return sourceStream;
    }
}
