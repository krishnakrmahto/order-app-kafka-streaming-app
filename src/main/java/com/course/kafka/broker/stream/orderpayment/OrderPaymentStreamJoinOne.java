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

@Configuration
public class OrderPaymentStreamJoinOne
{
    @Bean
    public KStream<String, OnlineOrderMessage> kStreamOnlineOrder(StreamsBuilder streamsBuilder,
                                                                  OnlineOrderTimestampExtractor orderTimestampExtractor,
                                                                  OnlinePaymentTimestampExtractor onlinePaymentTimestampExtractor)
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
                                                                                                  onlinePaymentTimestampExtractor,
                                                                                                  null));
        orderStream.join(paymentStream,
                         this::orderPaymentJoiner,
                         JoinWindows.of(Duration.ofHours(1)),
                         StreamJoined.with(stringSerde, orderSerde, paymentSerde))
                   .to("t.commodity.join-order-payment-one", Produced.with(stringSerde, new JsonSerde<>(OnlineOrderPaymentMessage.class)));

        return orderStream;
    }

    private OnlineOrderPaymentMessage orderPaymentJoiner(OnlineOrderMessage orderMessage, OnlinePaymentMessage paymentMessage)
    {
        return OnlineOrderPaymentMessage.builder()
                                .onlineOrderNumber(orderMessage.getOnlineOrderNumber())
                                .orderDateTime(orderMessage.getOrderDateTime())
                                .paymentDateTime(paymentMessage.getPaymentDateTime())
                                .paymentMethod(paymentMessage.getPaymentMethod())
                                .totalAmount(orderMessage.getTotalAmount())
                                .username(orderMessage.getUsername())
                                .build();
    }
}
