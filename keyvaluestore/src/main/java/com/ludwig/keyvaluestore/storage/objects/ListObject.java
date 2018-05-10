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
package com.ludwig.keyvaluestore.storage.objects;

import com.ludwig.keyvaluestore.Converter;
import com.ludwig.keyvaluestore.types.ListType;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;

import java.lang.reflect.Type;
import java.util.List;

public interface ListObject {
    <T> Single<List<T>> get(Converter converter, Type type);

    @NonNull
    <T> Single<List<T>> remove(
            @NonNull ListType.PredicateFunc<T> predicateFunc,
            Converter converter,
            Type type);

    @NonNull
    @SuppressWarnings("unchecked")
    <T> Single<List<T>> removeAll(
            @NonNull ListType.PredicateFunc<T> predicateFunc,
            Converter converter, Type type);

    @NonNull
    @SuppressWarnings("unchecked")
    <T> Single<List<T>> remove(int position, Converter converter, Type type);

    @NonNull
    <T> Single<List<T>> put(Converter converter, Type type, List<T> list);

    @NonNull
    <T> Observable<List<T>> observe(Converter converter, Type type);

    @NonNull
    <T> Single<List<T>> clear();

    @NonNull
    <T> Single<List<T>> append(T value, Converter converter, Type type);

    @NonNull
    <T> Single<List<T>> replace(T value, ListType.PredicateFunc<T> predicateFunc, Converter converter, Type type);

    @NonNull
    <T> Single<List<T>> addOrReplace(T value, ListType.PredicateFunc<T> predicateFunc, Converter converter, Type type);
}
