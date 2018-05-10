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
import com.ludwig.keyvaluestore.types.ListType;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.subjects.PublishSubject;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ListObjectV1 implements ListObject {
    private final PublishSubject updateSubject = PublishSubject.create();
    @NonNull
    private Store store;

    public ListObjectV1(@NonNull Store store) {
        this.store = store;
    }

    @Override
    public <T> Single<List<T>> get(Converter converter, Type type) {
        return Single.create(emitter -> store.runInReadLock(() -> {
            if (!store.exists()) {
                emitter.onSuccess(Collections.<T>emptyList());
                return;
            }

            List<T> list = converter.read(store, type);
            if (list == null) list = Collections.emptyList();
            emitter.onSuccess(list);
        }));
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> put(Converter converter, Type type, List<T> list) {
        return Single.create(emitter -> store.runInWriteLock(() -> {
                    if (!store.exists() && !store.createNew()) {
                        throw new IOException("Could not create file for store.");
                    }

                    store.converterWrite(list, converter, type);
                    emitter.onSuccess(list);
                    updateSubject.onNext(list);
                }));
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> Observable<List<T>> observe(Converter converter, Type type) {
        return updateSubject.startWith(get(converter, type).toObservable()).hide();
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> clear() {
        return Single.create(emitter -> store.runInWriteLock(() -> {
            if (store.exists() && !store.delete()) {
                throw new IOException("Clear operation on store failed.");
            }

            emitter.onSuccess(Collections.<T>emptyList());
            updateSubject.onNext(Collections.<T>emptyList());
        }));
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> append(T value, Converter converter, Type type) {
        return Single.create(emitter -> store.runInWriteLock(() -> {
            if (!store.exists() && !store.createNew()) {
                throw new IOException("Could not create file for store.");
            }

            List<T> originalList = converter.read(store, type);
            if (originalList == null) originalList = Collections.emptyList();

            List<T> result = new ArrayList<T>(originalList.size() + 1);
            result.addAll(originalList);
            result.add(value);

            store.converterWrite(result, converter, type);
            emitter.onSuccess(result);
            updateSubject.onNext(result);
        }));
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> replace(
            T value,
            ListType.PredicateFunc<T> predicateFunc,
            Converter converter,
            Type type) {
        return Single.create(emitter -> store.runInWriteLock(() -> {
            if (!store.exists()) {
                emitter.onSuccess(Collections.<T>emptyList());
                return;
            }

            List<T> originalList = converter.read(store, type);
            if (originalList == null) originalList = Collections.emptyList();

            int indexOfItemToReplace = -1;

            for (int i = 0; i < originalList.size(); i++) {
                if (predicateFunc.test(originalList.get(i))) {
                    indexOfItemToReplace = i;
                    break;
                }
            }

            List<T> modifiedList = new ArrayList<T>(originalList);

            if (indexOfItemToReplace != -1) {
                modifiedList.remove(indexOfItemToReplace);
                modifiedList.add(indexOfItemToReplace, value);
                store.converterWrite(modifiedList, converter, type);
            }

            emitter.onSuccess(modifiedList);
            updateSubject.onNext(modifiedList);
        }));
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> addOrReplace(
            T value,
            ListType.PredicateFunc<T> predicateFunc,
            Converter converter, Type type) {
        return Single.create(emitter -> store.runInWriteLock(() -> {
            if (!store.exists() && !store.createNew()) {
                throw new IOException("Could not create store.");
            }

            List<T> originalList = converter.read(store, type);
            if (originalList == null) originalList = Collections.emptyList();

            int indexOfItemToReplace = -1;

            for (int i = 0; i < originalList.size(); i++) {
                if (predicateFunc.test(originalList.get(i))) {
                    indexOfItemToReplace = i;
                    break;
                }
            }

            int modifiedListSize = indexOfItemToReplace == -1 ? originalList.size() + 1 :
                    originalList.size();

            List<T> modifiedList = new ArrayList<T>(modifiedListSize);
            modifiedList.addAll(originalList);

            if (indexOfItemToReplace == -1) {
                modifiedList.add(value);
            } else {
                modifiedList.remove(indexOfItemToReplace);
                modifiedList.add(indexOfItemToReplace, value);
            }

            store.converterWrite(modifiedList, converter, type);
            emitter.onSuccess(modifiedList);
            updateSubject.onNext(modifiedList);
        }));
    }


    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> remove(
            @NonNull final ListType.PredicateFunc<T> predicateFunc,
            Converter converter, Type type) {

        return Single.create(emitter -> store.runInWriteLock(() -> {
            if (!store.exists()) {
                emitter.onSuccess(Collections.<T>emptyList());
                return;
            }

            List<T> originalList = converter.read(store, type);
            if (originalList == null) originalList = Collections.emptyList();

            List<T> modifiedList = new ArrayList<T>(originalList);

            boolean removed = false;
            final Iterator<T> each = modifiedList.iterator();
            while (each.hasNext()) {
                if (predicateFunc.test(each.next())) {
                    each.remove();
                    removed = true;
                    break;
                }
            }


            if (removed) {
                store.converterWrite(modifiedList, converter, type);
            }

            emitter.onSuccess(modifiedList);
            updateSubject.onNext(modifiedList);
        }));
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> removeAll(
            @NonNull final ListType.PredicateFunc<T> predicateFunc,
            Converter converter, Type type) {

        return Single.create(emitter -> store.runInWriteLock(() -> {
            if (!store.exists()) {
                emitter.onSuccess(Collections.<T>emptyList());
                return;
            }

            List<T> originalList = converter.read(store, type);
            if (originalList == null) originalList = Collections.emptyList();

            List<T> modifiedList = new ArrayList<T>(originalList);

            boolean removed = false;
            final Iterator<T> each = modifiedList.iterator();
            while (each.hasNext()) {
                if (predicateFunc.test(each.next())) {
                    each.remove();
                    removed = true;
                }
            }

            if (removed) {
                store.converterWrite(modifiedList, converter, type);
            }

            emitter.onSuccess(originalList);
            updateSubject.onNext(originalList);
        }));
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T> Single<List<T>> remove(int position, Converter converter, Type type) {
        return Single.create(emitter -> store.runInWriteLock(() -> {
            List<T> originalList = converter.read(store, type);
            if (originalList == null) originalList = Collections.emptyList();

            List<T> modifiedList = new ArrayList<T>(originalList);
            modifiedList.remove(position);

            store.converterWrite(modifiedList, converter, type);
            emitter.onSuccess(modifiedList);
            updateSubject.onNext(modifiedList);
        }));
    }
}
