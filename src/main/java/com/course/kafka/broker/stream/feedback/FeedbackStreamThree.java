package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.util.Util;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.stream.Collectors;

@Configuration
public class FeedbackStreamThree
{
    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedbackThree(StreamsBuilder streamsBuilder)
    {
        KStream<String, FeedbackMessage> sourceStream = streamsBuilder.stream("t.commodity.feedback", Consumed.with(Serdes.String(),
                                                                                                     new JsonSerde<>(FeedbackMessage.class)));


        new KafkaStreamBrancher<String, String>().branch((key, value) -> Util.POSITIVE_FEEDBACK_WORDS.contains(value),
                                                         kStream -> kStream.to("t.commodity.feedback-three-good"))
                                                 .branch((key, value) -> Util.NEGATIVE_FEEDBACK_WORDS.contains(value),
                                                         kStream -> kStream.to("t.commodity.feedback-three-bad"))
                                                 .onTopOf(sourceStream.flatMap(getFeedbackWordsMapper()));

        return sourceStream;
    }

    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> getFeedbackWordsMapper()
    {
        return (key, value) -> Arrays.stream(value.getFeedback().toLowerCase().split("\\s+"))
                                     .map(word -> KeyValue.pair(value.getLocation(), word))
                                     .collect(Collectors.toList());
    }
}
