package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderMessageForPattern;
import com.course.kafka.broker.message.OrderMessageForReward;
import com.course.kafka.util.CommodityStreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Base64;

@Configuration
@Slf4j
public class CommodityStreamFive
{
    @Bean
    public KStream<String, OrderMessage> kStreamOrderMessageFour(StreamsBuilder streamsBuilder)
    {
        Serde<String> stringSerde = Serdes.String();

        KStream<String, OrderMessage> sourceStream = streamsBuilder.stream(CommodityStreamUtil.TOPIC_COMMODITY_ORDER,
                                                                           Consumed.with(stringSerde, new JsonSerde<>(OrderMessage.class)));

        KStream<String, OrderMessage> maskedOrderStream = sourceStream.mapValues(CommodityStreamUtil::maskCreditCardNumber);

        Produced<String, OrderMessageForPattern> orderMessageForPatternProducer = Produced.with(stringSerde, new JsonSerde<>(OrderMessageForPattern.class));

        // 1. Sink streams - pattern
        new KafkaStreamBrancher<String, OrderMessageForPattern>().branch(getPlasticPredicate(),
                                                                         kStream -> kStream.to("t.commodity.pattern-five.plastic", orderMessageForPatternProducer))
                                                                 .defaultBranch(kStream -> kStream.to("t.commodity.pattern-five.notplastic", orderMessageForPatternProducer))
                                                                 .onTopOf(maskedOrderStream.mapValues(this::toOrderMessageForPattern));

        // 2. Sink stream - reward
        maskedOrderStream.filter((key, value) -> value.getQuantity() > CommodityStreamUtil.LARGE_QUANTITY_MIN_VALUE)
                         .filter(getExpensivePredicate())
                         .map(getNewKeyValueMapperForRewardStream())
                         .to("t.commodity.reward-five", Produced.with(stringSerde, new JsonSerde<>(OrderMessageForReward.class)));

        // 3. Sink stream - storage
        maskedOrderStream.selectKey(getBase64KeyValueMapper())
                         .to("t.commodity.storage-five", Produced.with(stringSerde, new JsonSerde<>(OrderMessage.class)));

        // 4. Sink (not a sink stream) - fraud --> not creating a down stream, making an api call to fraud detection app
        maskedOrderStream.filter((key, value) -> value.getOrderLocation().toUpperCase().startsWith("C"))
                         .foreach(this::reportFraud);

        return sourceStream;
    }

    private void reportFraud(String key, OrderMessage message)
    {
        log.info("Reporting fraud to the fraud app using its API call for key: {}, value: {}", key, message);
    }

    private KeyValueMapper<String, OrderMessage, String> getBase64KeyValueMapper()
    {
        return (key, value) -> Base64.getEncoder().encodeToString(value.getOrderNumber().getBytes());
    }

    private Predicate<String, OrderMessage> getExpensivePredicate()
    {
        return (key, value) -> value.getPrice() > 10;
    }

    private Predicate<String, OrderMessageForPattern> getPlasticPredicate()
    {
        return (key, value) -> value.getItemName().startsWith("Plastic");
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

    private KeyValueMapper<String, OrderMessage, KeyValue<String, OrderMessageForReward>> getNewKeyValueMapperForRewardStream()
    {
        return (key, value) -> KeyValue.pair(value.getOrderLocation(), toOrderMessageForReward(value));
    }
}
