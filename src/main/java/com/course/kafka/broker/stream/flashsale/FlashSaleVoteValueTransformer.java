package com.course.kafka.broker.stream.flashsale;

import com.course.kafka.broker.message.FlashSaleVoteMessage;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class FlashSaleVoteValueTransformer implements ValueTransformer<FlashSaleVoteMessage, FlashSaleVoteMessage>
{
    private final long voteStartTimeEpoch;
    private final long voteEndTimeEpoch;
    private ProcessorContext processorContext;

    public FlashSaleVoteValueTransformer(LocalDateTime voteStartTime, LocalDateTime voteEndTime)
    {
        this.voteStartTimeEpoch = voteStartTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        this.voteEndTimeEpoch = voteEndTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    @Override
    public void init(ProcessorContext context)
    {
        this.processorContext = context;
    }

    @Override
    public FlashSaleVoteMessage transform(FlashSaleVoteMessage value)
    {
        long currentEpochTimestamp = processorContext.timestamp();
        return currentEpochTimestamp >= voteStartTimeEpoch && currentEpochTimestamp <= voteEndTimeEpoch? value: null;
    }

    @Override
    public void close()
    {

    }
}
