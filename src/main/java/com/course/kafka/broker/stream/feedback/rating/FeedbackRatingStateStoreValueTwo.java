package com.course.kafka.broker.stream.feedback.rating;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class FeedbackRatingStateStoreValueTwo
{
    private Map<Integer, Long> ratingCountMap;
}
