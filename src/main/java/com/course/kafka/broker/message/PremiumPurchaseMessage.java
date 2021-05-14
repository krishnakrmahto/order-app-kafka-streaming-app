package com.course.kafka.broker.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class PremiumPurchaseMessage
{

    private String item;

    private String purchaseNumber;

    private String username;

}
