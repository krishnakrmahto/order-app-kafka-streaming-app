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

//@Configuration
public class FlashSaleVoteStreamOne
{
    @Bean
    public KStream<String, FlashSaleVoteMessage> kStreamFlashSaleOne(StreamsBuilder streamsBuilder)
    {
        Serde<String> stringSerde = Serdes.String();
        KStream<String, FlashSaleVoteMessage> sourceStream = streamsBuilder.stream("t.commodity.flashsale.vote",
                                                                                   Consumed.with(stringSerde, new JsonSerde<>(FlashSaleVoteMessage.class)));

        sourceStream.map((key, value) -> KeyValue.pair(value.getCustomerId(), value.getItemName()))
                    .to("t.commodity.flashsale.vote-user-item");

        streamsBuilder.table("t.commodity.flashsale.vote-user-item", Consumed.with(stringSerde, stringSerde))
                      .groupBy((userId, itemName) -> KeyValue.pair(itemName, itemName)).count().toStream()
                      .to("t.commodity.flashsale.vote-one-result", Produced.with(stringSerde, Serdes.Long()));

        return sourceStream;
    }
}
