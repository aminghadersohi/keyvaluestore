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
package com.ludwig.keyvaluestore.storage;

import com.ludwig.keyvaluestore.Converter;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;

import java.io.*;
import java.lang.reflect.Type;

public interface Store {
    @NonNull
    Reader reader() throws Exception;

    @NonNull
    Writer writer() throws Exception;

    @NonNull
    OutputStream output() throws Exception;

    @NonNull
    InputStream input() throws Exception;

    Single<Boolean> exists();

    Single<Boolean> createNew() throws Exception;

    Single<Boolean> delete() throws Exception;

    <T> Single<T> converterWrite(T value, Converter converter, Type type) throws Exception;

    void startRead();

    void endRead();

    void startWrite();

    void endWrite();
}
