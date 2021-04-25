package com.course.kafka.util;

import com.course.kafka.broker.message.OrderMessage;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

@UtilityClass
public class CommodityStreamUtil
{
    public  final String TOPIC_COMMODITY_ORDER = "t.commodity.order";
    public final int LARGE_QUANTITY_MIN_VALUE = 5;
    public final Set<String> POSITIVE_FEEDBACK_WORDS = Set.of("happy", "good", "helpful");

    public OrderMessage maskCreditCardNumber(OrderMessage orderMessage)
    {
        orderMessage.setCreditCardNumber(orderMessage.getCreditCardNumber().replaceFirst("\\d{12}",
                                                                                         StringUtils.repeat("*", 12)));
        return orderMessage;
    }
}
