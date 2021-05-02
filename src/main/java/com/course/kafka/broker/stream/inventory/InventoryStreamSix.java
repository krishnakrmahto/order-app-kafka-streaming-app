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
public class InventoryStreamSix
{
    @Bean
    public KStream<String, InventoryMessage> kStreamInventory(StreamsBuilder streamsBuilder,
                                                              InventoryTimestampExtractor timestampExtractor)
    {
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        JsonSerde<InventoryMessage> inventoryMessageSerde = new JsonSerde<>(InventoryMessage.class);

        Duration windowLength = Duration.ofHours(1);
        Duration hopLength = Duration.ofMinutes(20);

        KStream<String, InventoryMessage> sourceStream = streamsBuilder.stream("t.commodity.inventory",
                                                                               Consumed.with(stringSerde,
                                                                                             inventoryMessageSerde,
                                                                                             timestampExtractor,
                                                                                             null));

        Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis());
        sourceStream.mapValues(value -> "ADD".equals(value.getType()) ? value.getQuantity() : -value.getQuantity())
                    .groupByKey()
                    .windowedBy(TimeWindows.of(windowLength).advanceBy(hopLength))
                    .reduce(Long::sum, Materialized.with(stringSerde, longSerde))
                    .toStream()
                    .through("t.commodity.inventory-total-six", Produced.with(windowedSerde, longSerde))
                    .print(Printed.toSysOut());

        return sourceStream;
    }
}
