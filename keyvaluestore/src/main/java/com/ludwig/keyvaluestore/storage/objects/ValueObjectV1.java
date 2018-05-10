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
        return Maybe.create(emitter -> store.runInWriteLock(
                () -> {
                    if (!store.exists()) {
                        emitter.onComplete();
                        return;
                    }

                    T value = converter.read(store, type);
                    if (value == null) {
                        emitter.onComplete();
                    }
                    emitter.onSuccess(value);
                }));
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Single<T> put(Converter converter, Type type, T value) {
        return Single.create(emitter -> store.runInWriteLock(
                () -> {
                    if (!store.exists() && !store.createNew()) {
                        throw new IOException("Could not create file for store.");
                    }

                    store.converterWrite(value, converter, type);
                    emitter.onSuccess(value);
                    updateSubject.onNext(new ValueUpdate<T>(value));
                }));
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Observable<ValueUpdate<T>> observe(Converter converter, Type type) {
        Observable<ValueUpdate<T>> startingValue = get(converter, type)
                .map(value -> new ValueUpdate<T>((T) value))
                .defaultIfEmpty(ValueUpdate.<T>empty())
                .toObservable();

        return updateSubject.startWith(startingValue).hide();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Completable clear() {
        return Completable.create(emitter -> store.runInWriteLock(
                () -> {
                    if (store.exists() && !store.delete()) {
                        throw new IOException("Clear operation on store failed.");
                    } else {
                        emitter.onComplete();
                    }

                    updateSubject.onNext(ValueUpdate.<T>empty());
                }));
    }
}
