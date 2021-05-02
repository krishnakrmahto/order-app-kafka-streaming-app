package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
public class InventoryStreamFive
{
    @Bean
    public KStream<String, InventoryMessage> kStreamInventoryThree(StreamsBuilder streamsBuilder,
                                                                   InventoryTimestampExtractor timestampExtractor)
    {
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        JsonSerde<InventoryMessage> inventoryMessageSerde = new JsonSerde<>(InventoryMessage.class);

        Duration windowLength = Duration.ofHours(1L);

        KStream<String, InventoryMessage> sourceStream = streamsBuilder.stream("t.commodity.inventory",
                                                                               Consumed.with(stringSerde,
                                                                                             inventoryMessageSerde,
                                                                                             timestampExtractor,
                                                                                             null));

        Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis());
        sourceStream.mapValues(value -> "ADD".equals(value.getType()) ? value.getQuantity() : -value.getQuantity())
                    .groupByKey()
                    .windowedBy(TimeWindows.of(windowLength))
                    .reduce(Long::sum, Materialized.with(stringSerde, longSerde))
                    .toStream()
                    .to("t.commodity.inventory-total-five", Produced.with(windowedSerde, longSerde));
        return sourceStream;
    }
}
