package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderMessageForPattern;
import com.course.kafka.broker.message.OrderMessageForReward;
import com.course.kafka.util.CommodityStreamUtil;
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
public class CommodityStreamOne
{
    @Bean
    public KStream<String, OrderMessage> kStreamOrderMessageOneStream(StreamsBuilder streamsBuilder)
    {
        Serde<String> stringSerde = Serdes.String();

        KStream<String, OrderMessage> sourceStream = streamsBuilder.stream("t.commodity.order",
                                                                           Consumed.with(stringSerde, new JsonSerde<>(OrderMessage.class)));

        KStream<String, OrderMessage> maskedOrderStream = sourceStream.mapValues(CommodityStreamUtil::maskCreditCardNumber);

        maskedOrderStream.mapValues(this::toOrderMessageForPattern)
                         .to("t.commodity.pattern-one", Produced.with(stringSerde, new JsonSerde<>(OrderMessageForPattern.class)));

        maskedOrderStream.filter((key, value) -> value.getQuantity() > CommodityStreamUtil.LARGE_QUANTITY_MIN_VALUE)
                         .mapValues(this::toOrderMessageForReward)
                         .to("t.commodity.reward-one", Produced.with(stringSerde, new JsonSerde<>(OrderMessageForReward.class)));

        maskedOrderStream.to("t.commodity.storage-one", Produced.with(stringSerde, new JsonSerde<>(OrderMessage.class)));

        return sourceStream;
    }

    private OrderMessageForPattern toOrderMessageForPattern(OrderMessage orderMessage)
    {
        return OrderMessageForPattern.builder()
                                     .itemName(orderMessage.getItemName())
                                     .orderNumber(orderMessage.getOrderNumber())
                                     .orderDateTime(orderMessage.getOrderDateTime())
                                     .totalAmoumt(orderMessage.getPrice() * orderMessage.getQuantity())
                                     .orderLocation(orderMessage.getOrderLocation())
                                     .build();
    }

    private OrderMessageForReward toOrderMessageForReward(OrderMessage orderMessage)
    {
        return OrderMessageForReward.builder()
                                    .id(orderMessage.getId())
                                    .orderNumber(orderMessage.getOrderNumber())
                                    .orderId(orderMessage.getOrderId())
                                    .itemName(orderMessage.getItemName())
                                    .price(orderMessage.getPrice())
                                    .quantity(orderMessage.getQuantity())
                                    .orderDateTime(orderMessage.getOrderDateTime())
                                    .orderLocation(orderMessage.getOrderLocation())
                                    .build();
    }
}
