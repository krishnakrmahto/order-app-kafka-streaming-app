package com.course.kafka.util;

import com.course.kafka.broker.message.InventoryMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.ZoneId;
import java.util.Optional;

//@Component
public class InventoryTimestampExtractor implements TimestampExtractor
{
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime)
    {
        InventoryMessage inventoryMessage = (InventoryMessage) record.value();

        return Optional.ofNullable(inventoryMessage.getTransactionTime())
                .map(transactionTime -> transactionTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .orElseGet(record::timestamp);
    }
}
