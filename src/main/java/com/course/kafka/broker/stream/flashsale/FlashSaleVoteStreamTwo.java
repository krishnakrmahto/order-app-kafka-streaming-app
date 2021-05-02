package com.course.kafka.broker.stream.flashsale;

import com.course.kafka.broker.message.FlashSaleVoteMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

//@Configuration
public class FlashSaleVoteStreamTwo
{
    private final LocalDateTime voteStartTime = LocalDateTime.of(LocalDate.now(), LocalTime.of(8, 40));
    private final LocalDateTime voteEndTime = LocalDateTime.of(LocalDate.now(), LocalTime.of(8, 50));

    @Bean
    public KStream<String, FlashSaleVoteMessage> kStreamFlashSaleOne(StreamsBuilder streamsBuilder)
    {
        Serde<String> stringSerde = Serdes.String();
        KStream<String, FlashSaleVoteMessage> sourceStream = streamsBuilder.stream("t.commodity.flashsale.vote",
                                                                           Consumed.with(stringSerde, new JsonSerde<>(FlashSaleVoteMessage.class)));

        sourceStream.transformValues(() -> new FlashSaleVoteValueTransformer(voteStartTime, voteEndTime))
                    .filter((key, transformedValue) -> Optional.ofNullable(transformedValue).isPresent())
                    .map((key, value) -> KeyValue.pair(value.getCustomerId(), value.getItemName()))
                    .to("t.commodity.flashsale.vote-user-item");

        streamsBuilder.table("t.commodity.flashsale.vote-user-item", Consumed.with(stringSerde, stringSerde))
                      .groupBy((userId, itemName) -> KeyValue.pair(itemName, itemName)).count().toStream()
                      .to("t.commodity.flashsale.vote-two-result", Produced.with(stringSerde, Serdes.Long()));

        return sourceStream;
    }
}
