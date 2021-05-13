package com.course.kafka.util;

import com.course.kafka.broker.message.OnlinePaymentMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.Optional;

@Component
public class OnlinePaymentTimestampExtractor implements TimestampExtractor
{
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime)
    {
        OnlinePaymentMessage message = (OnlinePaymentMessage) record.value();

        return Optional.ofNullable(message.getPaymentDateTime())
                       .map(transactionTime -> transactionTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                       .orElseGet(record::timestamp);
    }
}
