package com.course.kafka.util;

import com.course.kafka.broker.message.WebLayoutVoteMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.Optional;

@Component
public class WebLayoutVoteTimestampExtractor implements TimestampExtractor
{
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime)
    {
        WebLayoutVoteMessage message = (WebLayoutVoteMessage) record.value();

        return Optional.ofNullable(message.getVoteDateTime())
                .map(dateTime -> dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .orElseGet(record::timestamp);
    }
}
