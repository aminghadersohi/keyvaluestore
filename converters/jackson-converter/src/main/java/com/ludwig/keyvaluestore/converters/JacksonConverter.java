package com.ludwig.keyvaluestore.converters;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ludwig.keyvaluestore.Converter;
import com.ludwig.keyvaluestore.ConverterException;
import com.ludwig.keyvaluestore.storage.Store;

import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.Type;

/**
 * A {@link Converter} that uses a Jackson {@link ObjectMapper} to get the
 * job done.
 */
public class JacksonConverter implements Converter {
    private final ObjectMapper objectMapper;

    public JacksonConverter() {
        this(new ObjectMapper());
    }

    public JacksonConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public <T> void write(T data, Type type, Store store) throws ConverterException {
        try {
            OutputStream outputStream = store.output();
            objectMapper.writeValue(outputStream, data);
            outputStream.close();
        } catch (Exception e) {
            throw new ConverterException(e);
        }
    }

    @Override
    public <T> T read(Store store, Type type) throws ConverterException {
        JavaType javaType = objectMapper.getTypeFactory().constructType(type);

        try {
            Reader reader = store.reader();
            T value;

            if (!reader.ready()) {
                value = null;
            } else {
                value = objectMapper.readValue(reader, javaType);
            }

            reader.close();
            return value;
        } catch (Exception e) {
            throw new ConverterException(e);
        }
    }
}
