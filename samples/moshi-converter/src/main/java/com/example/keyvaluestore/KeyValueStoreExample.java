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
package com.example.keyvaluestore;

import com.ludwig.keyvaluestore.KeyValueStore;
import com.ludwig.keyvaluestore.KeyValueStoreFactory;
import com.ludwig.keyvaluestore.converters.MoshiConverter;
import com.ludwig.keyvaluestore.storage.stores.FileStoreManager;
import com.ludwig.keyvaluestore.types.ListType;
import com.ludwig.keyvaluestore.types.ValueType;
import com.ludwig.keyvaluestore.types.ValueUpdate;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;

import java.util.List;

public class KeyValueStoreExample {
    public static void main(String[] args) {
        KeyValueStore store = KeyValueStoreFactory.build(new FileStoreManager("/tmp"), new MoshiConverter());
        ValueType<String> valueStore = store.value("value", String.class);
        ListType<String> listStore = store.list("list", String.class);

        valueStore.observe()
                .skip(1)
                .subscribe(new Observer<ValueUpdate<String>>() {
                    @Override
                    public void onSubscribe(Disposable d) {

                    }

                    @Override
                    public void onNext(ValueUpdate<String> stringValueUpdate) {
                        System.out.println("value update: " + stringValueUpdate.value);
                    }

                    @Override
                    public void onError(Throwable e) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });

        listStore.observe()
                .skip(1)
                .subscribe(new Observer<List<String>>() {
                    @Override
                    public void onSubscribe(Disposable d) {

                    }

                    @Override
                    public void onNext(List<String> strings) {
                        System.out.println("list update: " + strings);
                    }

                    @Override
                    public void onError(Throwable e) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });

        valueStore.observePut("value1").blockingGet();
        valueStore.observePut("value2").blockingGet();
        valueStore.observePut("value1").blockingGet();
        listStore.observeAdd("listvalue1").blockingGet();
        listStore.observeAdd("listvalue2").blockingGet();
        listStore.observeAdd("listvalue3").blockingGet();
        valueStore.observePut("value2").blockingGet();
        listStore.observeRemove(value -> value.equals("listvalue1")).blockingGet();
        listStore.observeClear().blockingGet();
        valueStore.observeClear().blockingGet();

        Runnable runnable = () -> {
            listStore.observeClear().blockingGet();
            for (int i = 0; i < 1000; i++) {
                final int _i = i;
                if (Math.random() <= 0.01) {
                    listStore.observeAdd("listvalue"+i).blockingGet();
                } else {
                    listStore.observeRemove(value -> value.equals("listvalue"+_i)).blockingGet();
                }
            }
        };

        Runnable runnable2 = () -> {
            listStore.observeClear().blockingGet();
            for (int i = 0; i < 1000; i++) {
                final int _i = i;
                if (Math.random() <= 0.01) {
                    listStore.observeAdd("listvalue"+i).blockingGet();
                } else {
                    listStore.observeRemove(value -> value.equals("listvalue"+_i)).blockingGet();
                }
            }
        };

        Thread t = new Thread(runnable);
        t.start();
        Thread t2 = new Thread((runnable2));
        t2.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
