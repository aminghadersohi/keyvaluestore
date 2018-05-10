package com.ludwig.keyvaluestore.storage;

import com.ludwig.keyvaluestore.storage.objects.ListObject;
import com.ludwig.keyvaluestore.storage.objects.ListObjectV1;
import com.ludwig.keyvaluestore.storage.objects.ValueObject;
import com.ludwig.keyvaluestore.storage.objects.ValueObjectV1;

public final class ObjectFactory {
    public static ListObject list(Store store) {
        return new ListObjectV1(store);
    }

    public static ValueObject value(Store store) {
        return new ValueObjectV1(store);
    }
}
