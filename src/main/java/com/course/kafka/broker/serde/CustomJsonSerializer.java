package com.course.kafka.broker.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class CustomJsonSerializer<T> implements Serializer<T>
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data)
    {
        try {
            return objectMapper.writeValueAsBytes(data);
        }
        catch(JsonProcessingException e) {
            throw new SerializationException("Unable to serialise data into bytes", e);
        }
    }
}
