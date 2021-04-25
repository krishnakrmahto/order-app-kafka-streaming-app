package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.stream.Collectors;

@Configuration
public class FeedbackStreamTwo
{
    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedbackTwo(StreamsBuilder streamsBuilder)
    {
        KStream<String, FeedbackMessage> sourceStream = streamsBuilder.stream("t.commodity.feedback", Consumed.with(Serdes.String(),
                                                                                                     new JsonSerde<>(FeedbackMessage.class)));

        sourceStream.flatMap(getPositiveFeedbackKeyValueMapper())
                    .to("t.commodity.feedback-one-good", Produced.with(Serdes.String(), Serdes.String()));

        return sourceStream;
    }

    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> getPositiveFeedbackKeyValueMapper()
    {
        return (key, value) -> Arrays.stream(value.getFeedback().toLowerCase().split("\\s+"))
                                     .filter(CommodityStreamUtil.POSITIVE_FEEDBACK_WORDS::contains)
                                     .map(positiveFeedbackWord -> KeyValue.pair(value.getLocation(), positiveFeedbackWord))
                                     .collect(Collectors.toList());
    }
}
