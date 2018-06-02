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
package com.ludwig.keyvaluestore.types;

import com.ludwig.keyvaluestore.Converter;
import com.ludwig.keyvaluestore.storage.storable.ListStorable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.schedulers.Schedulers;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;


final class ListTypeV1<T> implements ListType<T> {
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final ListStorable storage;
    private final Converter converter;
    private final Type type;

    ListTypeV1(@NonNull ListStorable storage, @NonNull Converter converter, @NonNull Type type) {
        this.storage = storage;
        this.converter = converter;
        this.type = new ListTypeWrapper(type);
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> get() {
        return storage.get(converter, type);
    }

    @Override
    @NonNull
    public List<T> blockingGet() {
        return get().blockingGet();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> observePut(@NonNull final List<T> list) {
        return storage.put(converter, type, list);
    }

    @Override
    public void put(@NonNull List<T> list) {
        put(list, Schedulers.io());
    }

    @Override
    public void put(@NonNull List<T> list, @NonNull Scheduler scheduler) {
        observePut(list).subscribeOn(scheduler).subscribe();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Observable<List<T>> observe() {
        return storage.observe(converter, type);
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> observeClear() {
        return storage.clear();
    }

    @Override
    public void clear() {
        clear(Schedulers.io());
    }

    @Override
    public void clear(@NonNull Scheduler scheduler) {
        observeClear().subscribeOn(scheduler).subscribe();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> observeAdd(@NonNull final T value) {
        return storage.append(value, converter, type);

    }

    @Override
    public void add(@NonNull T value) {
        add(value, Schedulers.io());
    }

    @Override
    public void add(@NonNull T value, @NonNull Scheduler scheduler) {
        observeAdd(value).subscribeOn(scheduler).subscribe();
    }

    @Override
    public Single<List<T>> observeRemoveAll(@NonNull PredicateFunc<T> predicateFunc) {
        return storage.removeAll(predicateFunc, converter, type);
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> observeRemove(
            @NonNull final PredicateFunc<T> predicateFunc) {
        return storage.remove(predicateFunc, converter, type);
    }

    @Override
    public void remove(@NonNull PredicateFunc<T> predicateFunc) {
        remove(Schedulers.io(), predicateFunc);
    }


    @Override
    public void remove(@NonNull Scheduler scheduler,
                       @NonNull PredicateFunc<T> predicateFunc) {
        observeRemove(predicateFunc).subscribeOn(scheduler).subscribe();
    }

    @Override
    @NonNull
    public Single<List<T>> observeRemove(@NonNull final T value) {
        return observeRemove(valueToRemove -> value.equals(valueToRemove));
    }

    @Override
    public void remove(@NonNull final T value) {
        remove(value, Schedulers.io());
    }

    @Override
    public void remove(@NonNull final T value, @NonNull Scheduler scheduler) {
        observeRemove(value).subscribeOn(scheduler).subscribe();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> observeRemove(final int position) {
        return storage.remove(position, converter, type);
    }

    @Override
    public void remove(int position) {
        remove(position, Schedulers.io());
    }

    @Override
    public void remove(int position, @NonNull Scheduler scheduler) {
        observeRemove(position).subscribeOn(scheduler).subscribe();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> observeReplace(@NonNull final T value,
                                          @NonNull final PredicateFunc<T> predicateFunc) {

        return storage.replace(value, predicateFunc, converter, type);
    }

    @Override
    public void replace(@NonNull T value, @NonNull PredicateFunc<T> predicateFunc) {
        replace(value, Schedulers.io(), predicateFunc);
    }

    @Override
    public void replace(@NonNull T value, @NonNull Scheduler scheduler,
                        @NonNull PredicateFunc<T> predicateFunc) {
        observeReplace(value, predicateFunc).subscribeOn(scheduler).subscribe();
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public Single<List<T>> observeAddOrReplace(@NonNull final T value,
                                               @NonNull final PredicateFunc<T> predicateFunc) {
        return storage.addOrReplace(value, predicateFunc, converter, type);
    }

    @Override
    public void addOrReplace(@NonNull T value, @NonNull PredicateFunc<T> predicateFunc) {
        addOrReplace(value, Schedulers.io(), predicateFunc);
    }

    @Override
    public void addOrReplace(@NonNull T value, @NonNull Scheduler scheduler,
                             @NonNull PredicateFunc<T> predicateFunc) {
        observeAddOrReplace(value, predicateFunc).subscribeOn(scheduler).subscribe();
    }

    static final class ListTypeWrapper implements ParameterizedType {
        private final Type wrappedType;

        ListTypeWrapper(Type wrappedType) {
            this.wrappedType = wrappedType;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return new Type[]{wrappedType};
        }

        @Override
        public Type getOwnerType() {
            return null;
        }

        @Override
        public Type getRawType() {
            return List.class;
        }
    }
}