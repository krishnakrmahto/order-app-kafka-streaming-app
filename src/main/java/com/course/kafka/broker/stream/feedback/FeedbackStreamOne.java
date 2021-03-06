package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.util.Util;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.stream.Collectors;

//@Configuration
public class FeedbackStreamOne
{
    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedback(StreamsBuilder streamsBuilder)
    {
        KStream<String, FeedbackMessage> sourceStream = streamsBuilder.stream("t.commodity.feedback", Consumed.with(Serdes.String(),
                                                                                                     new JsonSerde<>(FeedbackMessage.class)));

        sourceStream.flatMapValues(getPositiveFeedBackValueMapper())
                    .to("t.commodity.feedback-one-good", Produced.with(Serdes.String(), Serdes.String()));

        return sourceStream;
    }

    private ValueMapper<FeedbackMessage, Iterable<String>> getPositiveFeedBackValueMapper()
    {
        return feedbackMessage -> Arrays.stream(feedbackMessage.getFeedback()
                                                               .toLowerCase()
                                                               .split("\\s+"))
                                        .filter(Util.POSITIVE_FEEDBACK_WORDS::contains)
                                        .collect(Collectors.toList());
    }
}
