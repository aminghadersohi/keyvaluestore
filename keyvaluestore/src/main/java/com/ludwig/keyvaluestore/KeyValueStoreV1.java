/*
 * Copyright (C) 2018 Ludwig
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ludwig.keyvaluestore;

import com.ludwig.keyvaluestore.storage.StoreManager;
import com.ludwig.keyvaluestore.types.ListType;
import com.ludwig.keyvaluestore.types.TypeFactory;
import com.ludwig.keyvaluestore.types.ValueType;
import io.reactivex.annotations.NonNull;

import java.lang.reflect.Type;

class KeyValueStoreV1 implements KeyValueStore {

    @NonNull
    private final StoreManager storeManager;
    @NonNull
    private final Converter converter;

    KeyValueStoreV1(@NonNull StoreManager storeManager, @NonNull Converter converter) {
        this.storeManager = storeManager;
        this.converter = converter;
    }

    @Override
    @NonNull
    public <T> ValueType<T> value(@NonNull String key, @NonNull Type type) {
        return TypeFactory.build(storeManager.valueStorage(key), converter, type);
    }

    @Override
    @NonNull
    public <T> ListType<T> list(@NonNull String key, @NonNull Type type) {
        return TypeFactory.build(storeManager.listStorage(key), converter, type);
    }
}
