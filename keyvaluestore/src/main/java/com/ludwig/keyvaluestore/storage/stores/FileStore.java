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

import com.ludwig.keyvaluestore.Converter;
import com.ludwig.keyvaluestore.storage.Store;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;

import java.io.*;
import java.lang.reflect.Type;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileStore implements Store {
    protected final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    @NonNull private File file;
    private Integer readCount;

    FileStore(@NonNull File file) {
        this.file = file;
    }

    @Override
    @NonNull
    public Writer writer() throws IOException {
        return new FileWriter(this.file);
    }

    @Override
    @NonNull
    public Reader reader() throws IOException {
        return new FileReader(this.file);
    }

    @Override
    public OutputStream output() throws Exception {
        return new FileOutputStream(this.file);
    }

    @Override
    public InputStream input() throws Exception {
        return new FileInputStream(this.file);
    }

    @Override
    public Single<Boolean> exists() {
        return Single.fromCallable(() -> FileStore.this.file.exists());
    }

    @Override
    public Single<Boolean> createNew() {
        return Single.fromCallable(() -> FileStore.this.file.createNewFile());
    }

    @Override
    public Single<Boolean> delete() {
        return Single.fromCallable(() -> FileStore.this.file.delete());
    }

    @Override
    public <T> Single<T> converterWrite(T value, Converter converter, Type type) {
        return createTemp()
                .map(tmpFile -> {
                    converter.write(value, type, tmpFile);
                    return tmpFile;
                })
                .flatMap(tmpFile -> delete().flatMap(deleted -> {
                    if (!deleted) {
                        throw new IOException("Failed to write get to store.");
                    }
                    return set(tmpFile).map(isSet -> {
                        if (!isSet) {
                            throw new IOException("Failed to write get to store.");
                        }
                        return value;
                    });
                }));
    }

    @Override
    public void startRead() {
        readWriteLock.readLock().lock();
    }

    @Override
    public void endRead() {
        readWriteLock.readLock().unlock();
    }

    @Override
    public void startWrite() {
        Lock readLock = readWriteLock.readLock();
        readCount = readWriteLock.getWriteHoldCount() == 0 ? readWriteLock.getReadHoldCount() : 0;

        for (int i = 0; i < readCount; i++) {
            readLock.unlock();
        }

        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
    }

    @Override
    public void endWrite() {
        Lock readLock = readWriteLock.readLock();
        Lock writeLock = readWriteLock.writeLock();

        for (int i = 0; i < (readCount != null ? readCount : 0); i++) {
            readLock.lock();
        }
        readCount = null;
        writeLock.unlock();

    }

    private Single<FileStore> createTemp() {
        return Single.fromCallable(() -> new FileStore(new File(this.file.getAbsolutePath() + ".tmp")));
    }

    private Single<Boolean> set(@NonNull FileStore storage) {
        return Single.fromCallable(() -> storage.file.renameTo(this.file));
    }

}