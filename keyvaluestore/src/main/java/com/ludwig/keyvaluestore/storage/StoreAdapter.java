package com.ludwig.keyvaluestore.storage;

import io.reactivex.Completable;
import io.reactivex.Single;

public interface StoreAdapter {

  Completable write(String key, String value);

  Single<String> read(String key);

  Single<Boolean> exists(String key);

  Single<Boolean> createNew(String key);

  Single<Boolean> delete(String key);
}
