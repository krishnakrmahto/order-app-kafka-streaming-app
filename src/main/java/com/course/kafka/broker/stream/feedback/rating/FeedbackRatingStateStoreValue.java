package com.course.kafka.broker.stream.feedback.rating;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class FeedbackRatingStateStoreValue
{
    private long count;
    private long sumRating;
}
