package com.course.kafka.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.LocalDateTime;

import static com.course.kafka.util.DateConstant.DATE_TIME_FORMATTER;

public class LocalDateTimeSerializer extends StdSerializer<LocalDateTime> {

    private static final long serialVersionUID = 1L;

    public LocalDateTimeSerializer()
    {
        this(null);
    }
    protected LocalDateTimeSerializer(Class<LocalDateTime> t)
    {
        super(t);
    }

    @Override
    public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider arg2) throws IOException {
        gen.writeString(DATE_TIME_FORMATTER.format(value));
    }
}
