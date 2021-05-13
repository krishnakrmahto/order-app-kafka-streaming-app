package com.course.kafka.broker.stream.orderpayment;

import com.course.kafka.broker.message.OnlineOrderMessage;
import com.course.kafka.broker.message.OnlineOrderPaymentMessage;
import com.course.kafka.broker.message.OnlinePaymentMessage;
import com.course.kafka.util.OnlineOrderTimestampExtractor;
import com.course.kafka.util.OnlinePaymentTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.Optional;

@Configuration
public class OrderPaymentStreamJoinTwo
{
    @Bean
    public KStream<String, OnlineOrderMessage> kStreamOnlineOrder(StreamsBuilder streamsBuilder,
                                                                  OnlineOrderTimestampExtractor orderTimestampExtractor,
                                                                  OnlinePaymentTimestampExtractor paymentTimestampExtractor)
    {
        Serde<String> stringSerde = Serdes.String();

        JsonSerde<OnlineOrderMessage> orderSerde = new JsonSerde<>(OnlineOrderMessage.class);
        JsonSerde<OnlinePaymentMessage> paymentSerde = new JsonSerde<>(OnlinePaymentMessage.class);

        KStream<String, OnlineOrderMessage> orderStream = streamsBuilder.stream("t.commodity.online-order",
                                                                                Consumed.with(stringSerde,
                                                                                              orderSerde,
                                                                                              orderTimestampExtractor,
                                                                                              null));

        KStream<String, OnlinePaymentMessage> paymentStream = streamsBuilder.stream("t.commodity.online-payment",
                                                                                    Consumed.with(stringSerde,
                                                                                                  paymentSerde,
                                                                                                  paymentTimestampExtractor,
                                                                                                  null));

        orderStream.leftJoin(paymentStream,
                         this::orderPaymentJoiner,
                         JoinWindows.of(Duration.ofHours(1)),
                         StreamJoined.with(stringSerde, orderSerde, paymentSerde))
                   .to("t.commodity.join-order-payment-two", Produced
                           .with(stringSerde, new JsonSerde<>(OnlineOrderPaymentMessage.class)));

        return orderStream;
    }

    private OnlineOrderPaymentMessage orderPaymentJoiner(OnlineOrderMessage orderMessage, OnlinePaymentMessage paymentMessage)
    {
        OnlineOrderPaymentMessage.OnlineOrderPaymentMessageBuilder orderPaymentMessageBuilder =
                OnlineOrderPaymentMessage.builder()
                                         .onlineOrderNumber(orderMessage.getOnlineOrderNumber())
                                         .orderDateTime(orderMessage.getOrderDateTime())
                                         .totalAmount(orderMessage.getTotalAmount())
                                         .username(orderMessage.getUsername());

        // right stream can be null in left join, so the nullable check
        Optional.ofNullable(paymentMessage).ifPresent(message -> orderPaymentMessageBuilder.paymentDateTime(paymentMessage.getPaymentDateTime())
                                                                                           .paymentMethod(paymentMessage.getPaymentMethod())
                                                                                           .build());

        return orderPaymentMessageBuilder.build();
    }
}
