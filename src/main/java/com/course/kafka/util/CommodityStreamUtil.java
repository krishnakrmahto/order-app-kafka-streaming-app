package com.course.kafka.util;

import com.course.kafka.broker.message.OrderMessage;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class CommodityStreamUtil
{
    public  final String TOPIC_COMMODITY_ORDER = "t.commodity.order";
    public int LARGE_QUANTITY_MIN_VALUE = 5;

    public OrderMessage maskCreditCardNumber(OrderMessage orderMessage)
    {
        orderMessage.setCreditCardNumber(orderMessage.getCreditCardNumber().replaceFirst("\\d{12}",
                                                                                         StringUtils.repeat("*", 12)));
        return orderMessage;
    }
}
