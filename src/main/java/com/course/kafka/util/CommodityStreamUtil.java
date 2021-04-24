package com.course.kafka.util;

import com.course.kafka.broker.message.OrderMessage;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CommodityStreamUtil
{
    public OrderMessage maskCreditCardNumber(OrderMessage orderMessage)
    {
        orderMessage.setCreditCardNumber(orderMessage.getCreditCardNumber().replaceFirst("\\d{12}", "x"));
        return orderMessage;
    }
}
