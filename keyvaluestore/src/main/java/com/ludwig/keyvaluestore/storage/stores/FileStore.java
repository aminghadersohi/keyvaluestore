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
import com.ludwig.keyvaluestore.storage.ThrowingRunnable;
import io.reactivex.annotations.NonNull;

import java.io.*;
import java.lang.reflect.Type;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileStore implements Store {
    protected final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    @NonNull private File file;

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
    public boolean exists() {
        return this.file.exists();
    }

    @Override
    public boolean createNew() throws IOException {
        return this.file.createNewFile();
    }

    @Override
    public boolean delete() throws IOException {
        return this.file.delete();
    }

    @Override
    public <T> void converterWrite(T value, Converter converter, Type type)
            throws IOException {
        FileStore tmpFile = createTemp();
        converter.write(value, type, tmpFile);

        if (!delete() || !set(tmpFile)) {
            throw new IOException("Failed to write get to file.");
        }
    }

    @Override
    public void runInReadLock(ThrowingRunnable runnable) {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();

        try {
            runnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void runInWriteLock(ThrowingRunnable runnable) {
        Lock readLock = readWriteLock.readLock();
        int readCount = readWriteLock.getWriteHoldCount() == 0 ? readWriteLock.getReadHoldCount() : 0;

        for (int i = 0; i < readCount; i++) {
            readLock.unlock();
        }

        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();

        try {
            runnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            for (int i = 0; i < readCount; i++) {
                readLock.lock();
            }
            writeLock.unlock();
        }
    }

    private FileStore createTemp() {
        return new FileStore(new File(this.file.getAbsolutePath() + ".tmp"));
    }

    private boolean set(@NonNull FileStore storage) {
        return storage.file.renameTo(this.file);
    }

}