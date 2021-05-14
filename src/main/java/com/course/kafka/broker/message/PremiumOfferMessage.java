package com.course.kafka.broker.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class PremiumOfferMessage {

    private String username;

    private String level;

    private String purchaseNumber;

}
