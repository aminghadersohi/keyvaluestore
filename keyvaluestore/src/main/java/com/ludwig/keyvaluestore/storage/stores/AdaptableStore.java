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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.ludwig.keyvaluestore.Converter;
import com.ludwig.keyvaluestore.storage.Store;
import com.ludwig.keyvaluestore.storage.StoreAdapter;
import io.reactivex.Single;
import io.reactivex.annotations.Nullable;
import java.io.*;
import java.lang.reflect.Type;

public class AdaptableStore implements Store {
  private final String key;
  private final StoreAdapter storeAdapter;

  AdaptableStore(String key, StoreAdapter storeAdapter) {
    this.key = key;
    this.storeAdapter = storeAdapter;
  }

  @Override
  public Reader reader() {
    return new Reader() {
      private final Object closeLock = new Object();
      @Nullable private volatile String buffer;

      @Override
      public int read(char[] b, int off, int len) {
        if (buffer == null) {
          buffer = storeAdapter.read(key).blockingGet();
        }
        int read = 0;
        for (int i = off; i < buffer.length() && read < len; i++, read++) {
          b[i] = buffer.charAt(i);
        }
        return read;
      }

      @Override
      public void close() {
        synchronized (closeLock) {
          if (buffer == null) {
            return;
          }
          buffer = null;
        }
      }
    };
  }

  @Override
  public InputStream input() {
    return new InputStream() {
      @Nullable private volatile String buffer;
      private volatile boolean closed = false;

      @Override
      public int read() throws IOException {
        byte[] b = new byte[1];
        return (read(b, 0, 1) != -1) ? b[0] & 0xff : -1;
      }

      @Override
      public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
      }

      @Override
      public int read(byte b[], int off, int len) throws IOException {
        if (closed && len > 0) {
          throw new IOException("Stream Closed");
        }
        if (buffer == null) {
          buffer = storeAdapter.read(key).blockingGet();
        }
        int read = 0;
        for (int i = off; i < buffer.length() && read < len; i++, read++) {
          b[i] = (byte) buffer.charAt(i);
        }
        return read;
      }

      @Override
      public long skip(long n) throws IOException {
        if (closed) {
          throw new IOException("Stream Closed");
        }
        buffer = null;
        return 0;
      }
    };
  }

  @Override
  public Writer writer() {
    return new Writer() {
      private final Object closeLock = new Object();
      private volatile boolean closed = false;
      private StringBuilder buffer = new StringBuilder();

      @Override
      public void write(char[] b, int off, int len) {
        buffer.append(new String(b, off, len));
      }

      @Override
      public void flush() {
        storeAdapter.write(key, buffer.toString()).blockingAwait();
      }

      @Override
      public void close() {
        synchronized (closeLock) {
          if (closed) {
            return;
          }
          closed = true;
          buffer = new StringBuilder();
        }
      }
    };
  }

  @Override
  public OutputStream output() {
    return new OutputStream() {
      private final Object closeLock = new Object();
      StringBuilder buffer = new StringBuilder();
      private volatile boolean closed = false;

      @Override
      public void write(int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
      }

      @Override
      public void write(byte b[], int off, int len) throws IOException {
        if (closed && len > 0) {
          throw new IOException("Stream Closed");
        }
        buffer.append(new String(b, off, len, UTF_8));
      }

      @Override
      public void close() {
        synchronized (closeLock) {
          if (closed) {
            return;
          }
          storeAdapter.write(key, buffer.toString()).blockingAwait();
          closed = true;
          buffer = new StringBuilder();
        }
      }
    };
  }

  @Override
  public Single<Boolean> exists() {
    return storeAdapter.exists(key);
  }

  @Override
  public Single<Boolean> createNew() {
    return storeAdapter.createNew(key);
  }

  @Override
  public Single<Boolean> delete() {
    return storeAdapter.delete(key);
  }

  @Override
  public <T> Single<T> converterWrite(T value, Converter converter, Type type) {
    return Single.fromCallable(
        () -> {
          converter.write(value, type, AdaptableStore.this);
          return value;
        });
  }

  @Override
  public void startRead() {}

  @Override
  public void endRead() {}

  @Override
  public void startWrite() {}

  @Override
  public void endWrite() {}
}
