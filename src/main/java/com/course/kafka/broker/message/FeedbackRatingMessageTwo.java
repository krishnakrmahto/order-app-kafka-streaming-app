package com.course.kafka.broker.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FeedbackRatingMessageTwo
{
    private String location;
    private double averageRating;
    private Map<Integer, Long> ratingCountMap;
}
