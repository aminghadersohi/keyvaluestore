package com.ludwig.keyvaluestore.converters;

import com.ludwig.keyvaluestore.Converter;
import com.ludwig.keyvaluestore.ConverterException;
import com.ludwig.keyvaluestore.storage.Store;
import com.google.gson.Gson;

import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;

public class GsonConverter implements Converter {
    private Gson gson;

    public GsonConverter() {
        this(new Gson());
    }

    public GsonConverter(Gson gson) {
        this.gson = gson;
    }

    @Override
    public <T> void write(T data, Type type, Store store) throws ConverterException {
        try {
            Writer writer = store.writer();
            gson.toJson(data, type, writer);
            writer.close();
        } catch (Exception e) {
            throw new ConverterException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T read(Store store, Type type) throws ConverterException {
        try {
            Reader reader = store.reader();
            T value = gson.fromJson(reader, type);
            reader.close();
            return value;
        } catch (Exception e) {
            throw new ConverterException(e);
        }
    }
}