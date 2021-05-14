package com.course.kafka.util;

import com.course.kafka.broker.message.WebColorVoteMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.Optional;

@Component
public class WebColorVoteTimestampExtractor implements TimestampExtractor
{
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime)
    {
        WebColorVoteMessage message = (WebColorVoteMessage) record.value();

        return Optional.ofNullable(message.getVoteDateTime())
                .map(dateTime -> dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .orElseGet(record::timestamp);
    }
}
