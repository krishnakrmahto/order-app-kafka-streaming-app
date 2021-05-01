package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingMessageTwo;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class FeedbackRatingValueTransformerTwo implements ValueTransformer<FeedbackMessage, FeedbackRatingMessageTwo>
{
    private ProcessorContext processorContext;
    private final String stateStoreName = "feedbackRatingStateStoreTwo";
    private KeyValueStore<String, FeedbackRatingStateStoreValueTwo> ratingCountMapStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext processorContext)
    {
        this.processorContext = processorContext;
        this.ratingCountMapStore = (KeyValueStore<String, FeedbackRatingStateStoreValueTwo>) processorContext.getStateStore(stateStoreName);
    }

    @Override
    public FeedbackRatingMessageTwo transform(FeedbackMessage feedbackMessage)
    {
        FeedbackRatingStateStoreValueTwo stateStoreValue = Optional.ofNullable(ratingCountMapStore.get(feedbackMessage.getLocation()))
                                                                   .orElseGet(FeedbackRatingStateStoreValueTwo::new);

        int rating = feedbackMessage.getRating();
        Map<Integer, Long> ratingCountMap = Optional.ofNullable(stateStoreValue.getRatingCountMap())
                .orElseGet(() -> {
                    HashMap<Integer, Long> countMap = new HashMap<>();
                    countMap.put(rating, 0L);
                    return countMap;
                });

        Long newCountForRating = Optional.ofNullable(ratingCountMap.get(rating)).orElse(0L) + 1;
        ratingCountMap.put(rating, newCountForRating);

        ratingCountMapStore.put(feedbackMessage.getLocation(), new FeedbackRatingStateStoreValueTwo(ratingCountMap));

        return FeedbackRatingMessageTwo.builder()
                                       .averageRating(calculateAverageRating(ratingCountMap))
                                       .ratingCountMap(ratingCountMap)
                                       .location(feedbackMessage.getLocation())
                                       .build();
    }

    private double calculateAverageRating(Map<Integer, Long> ratingCountMap)
    {
        Long totalCount = 0L;
        Long totalRatingSum = 0L;
        for(Map.Entry<Integer, Long> ratingCountEntry: ratingCountMap.entrySet())
        {
            totalCount += ratingCountEntry.getValue();
            totalRatingSum += ratingCountEntry.getValue() * ratingCountEntry.getKey();
        }

        return Math.round((double) totalRatingSum / totalCount * 10d) / 10d;
    }

    @Override
    public void close()
    {

    }
}
