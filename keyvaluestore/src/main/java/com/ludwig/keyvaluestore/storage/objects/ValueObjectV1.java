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
import com.ludwig.keyvaluestore.storage.Store;
import com.ludwig.keyvaluestore.types.ValueUpdate;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.subjects.PublishSubject;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Optional;

public class ValueObjectV1 implements ValueObject {
    protected final PublishSubject updateSubject = PublishSubject.create();

    @NonNull
    private Store store;

    public ValueObjectV1(@NonNull Store store) {
        this.store = store;
    }

    @Override
    @NonNull
    public <T> Maybe<T> get(Converter converter, Type type) {
        return Completable.fromAction(store::startRead)
                .andThen(store.exists())
                .filter(Boolean::booleanValue)
                .map(exists -> Optional.ofNullable(converter.<T>read(store, type)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .doFinally(store::endRead);
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Single<T> put(Converter converter, Type type, T value) {
        return Completable.fromAction(store::startWrite)
                .andThen(store.exists())
                .flatMap(exists -> exists ? Single.just(true) : store.createNew())
                .flatMap(createSuccess -> {
                    if (!createSuccess) {
                        throw new IOException("Could not create store.");
                    }
                    return store.converterWrite(value, converter, type);
                })
                .doOnSuccess(o -> updateSubject.onNext(new ValueUpdate<>(value)))
                .doFinally(store::endWrite);
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Observable<ValueUpdate<T>> observe(Converter converter, Type type) {
        return updateSubject.startWith(get(converter, type)
                .map(value -> new ValueUpdate<>((T) value))
                .defaultIfEmpty(ValueUpdate.empty())
                .toObservable()).hide();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Completable clear() {
        return Completable.fromAction(store::startWrite)
                .andThen(store.exists())
                .filter(Boolean::booleanValue)
                .flatMapSingle(exists -> store.delete())
                .doOnSuccess(deleteSuccess -> {
                    if (!deleteSuccess) {
                        throw new IOException("Clear operation on store failed.");
                    }
                    updateSubject.onNext(ValueUpdate.<T>empty());
                })
                .doFinally(store::endWrite)
                .ignoreElement();
    }
}
