package com.ludwig.keyvaluestore.storage.storable;

import com.ludwig.keyvaluestore.storage.Store;

public final class StorableFactory {
    public static ListStorable list(Store store) {
        return new ListStorableV1(store);
    }

    public static ValueStorable value(Store store) {
        return new ValueStorableV1(store);
    }
}
