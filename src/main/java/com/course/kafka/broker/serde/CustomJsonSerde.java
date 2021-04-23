package com.course.kafka.broker.serde;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

@AllArgsConstructor
public class CustomJsonSerde<T> implements Serde<T>
{
    private final CustomJsonSerializer<T> serializer;
    private final CustomJsonDeserializer<T> deserializer;

    @Override
    public Serializer<T> serializer()
    {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer()
    {
        return deserializer;
    }
}
