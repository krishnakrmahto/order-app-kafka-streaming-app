package com.course.kafka.util;

import com.course.kafka.broker.message.OrderMessage;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class CommodityStreamUtil
{
    public OrderMessage maskCreditCardNumber(OrderMessage orderMessage)
    {
        orderMessage.setCreditCardNumber(orderMessage.getCreditCardNumber().replaceFirst("\\d{12}",
                                                                                         StringUtils.repeat("*", 12)));
        return orderMessage;
    }
}
