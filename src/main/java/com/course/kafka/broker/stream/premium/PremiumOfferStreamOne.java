package com.course.kafka.broker.stream.premium;

import com.course.kafka.broker.message.PremiumOfferMessage;
import com.course.kafka.broker.message.PremiumPurchaseMessage;
import com.course.kafka.broker.message.PremiumUserMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.List;

@Configuration
public class PremiumOfferStreamOne
{
    private final List<String> premiumLevels = Arrays.asList("gold", "silver");

    @Bean
    public KStream<String, PremiumOfferMessage> kStreamPremiumOffer(StreamsBuilder streamsBuilder)
    {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<PremiumUserMessage> premiumUserSerde = new JsonSerde<>(PremiumUserMessage.class);
        JsonSerde<PremiumPurchaseMessage> premiumPurchaseSerde = new JsonSerde<>(PremiumPurchaseMessage.class);
        JsonSerde<PremiumOfferMessage> offerSerde = new JsonSerde<>(PremiumOfferMessage.class);

        KStream<String, PremiumPurchaseMessage> premiumPurchaseStream = streamsBuilder.stream("t.commodity.premium-purchase",
                                                                                       Consumed.with(stringSerde, premiumPurchaseSerde))
                                                                               .selectKey((k, v) -> v.getUsername());

        KTable<String, PremiumUserMessage> premiumUserTable = streamsBuilder.table("t.commodity.premium-user",
                                                                         Consumed.with(stringSerde, premiumUserSerde))
                                                                  .filter(((key, value) -> premiumLevels
                                                                          .contains(value.getLevel())));

        KStream<String, PremiumOfferMessage> offerStream = premiumPurchaseStream.join(premiumUserTable, this::joiner,
                                                                               Joined.with(stringSerde,
                                                                                           premiumPurchaseSerde,
                                                                                           premiumUserSerde));
        offerStream.to("t.commodity.premium-offer-one", Produced.with(stringSerde, offerSerde));

        return offerStream;
    }

    private PremiumOfferMessage joiner(PremiumPurchaseMessage purchaseMessage, PremiumUserMessage userMessage)
    {
        return new PremiumOfferMessage(purchaseMessage.getUsername(),
                                       userMessage.getLevel(),
                                       purchaseMessage.getPurchaseNumber());
    }
}
