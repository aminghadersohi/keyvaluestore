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
package com.ludwig.keyvaluestore.storage.stores;

import com.ludwig.keyvaluestore.storage.*;
import com.ludwig.keyvaluestore.storage.objects.ListObject;
import com.ludwig.keyvaluestore.storage.objects.ListObjectV1;
import com.ludwig.keyvaluestore.storage.objects.ValueObject;
import io.reactivex.annotations.NonNull;

import java.io.File;

public class FileStoreManager implements StoreManager {

    @NonNull
    private final String basePath;

    public FileStoreManager(@NonNull String basePath) {
        this.basePath = basePath;
    }

    @NonNull
    @Override
    public ValueObject valueStorage(@NonNull String key) {
        return ObjectFactory.value(new FileStore(new File(basePath + "/" + key + ".json")));
    }

    @NonNull
    @Override
    public ListObject listStorage(String key) {
        return new ListObjectV1(new FileStore(new File(basePath + "/" + key + ".json")));
    }
}
