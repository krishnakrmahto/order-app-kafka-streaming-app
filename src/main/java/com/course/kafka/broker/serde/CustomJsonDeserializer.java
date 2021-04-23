package com.course.kafka.broker.serde;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@RequiredArgsConstructor
public class CustomJsonDeserializer<T> implements Deserializer<T>
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Class<T> targetType;

    @Override
    public T deserialize(String topic, byte[] data)
    {
        try {
            return objectMapper.readValue(data, targetType);
        }
        catch(IOException e) {
            throw new SerializationException(e);
        }
    }
}
