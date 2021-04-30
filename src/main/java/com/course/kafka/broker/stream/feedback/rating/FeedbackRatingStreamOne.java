package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackAverageRatingMessageOne;
import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class FeedbackRatingStreamOne
{
    private final String stateStoreName = "feedbackRatingStateStoreOne";
    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedbackRatingStreamOne(StreamsBuilder streamsBuilder,
                   StoreBuilder<KeyValueStore<String, FeedbackRatingStateStoreValue>> storeBuilder)
    {
        Serde<String> stringSerde = Serdes.String();

        KStream<String, FeedbackMessage> sourceStream = streamsBuilder.stream("t.commodity.feedback",
                                                                              Consumed.with(stringSerde, new JsonSerde<>(FeedbackMessage.class)));

        // this line seems to indicate that every topology should register/add an exclusive state store for itself
        streamsBuilder.addStateStore(storeBuilder);

        sourceStream.transformValues(() -> new FeedbackRatingTransformerOne(stateStoreName), stateStoreName)
                    .to("t.commodity.feedback.rating-one", Produced.with(stringSerde,
                                                                         new JsonSerde<>(FeedbackAverageRatingMessageOne.class)));
        return sourceStream;
    }

    @Bean
    public StoreBuilder<KeyValueStore<String, FeedbackRatingStateStoreValue>> feedbackRatingStateStoreBuilder(KeyValueBytesStoreSupplier storeSupplier)
    {
        return Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), new JsonSerde<>(FeedbackRatingStateStoreValue.class));
    }

    @Bean
    public KeyValueBytesStoreSupplier feedbackRatingStateStoreOne()
    {
        return Stores.inMemoryKeyValueStore(stateStoreName);
    }
}
