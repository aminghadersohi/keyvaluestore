package com.ludwig.keyvaluestore.converters

//import com.google.common.truth.Truth.assertThat
//import com.ludwig.keyvaluestore.KeyValueStore
//import com.ludwig.keyvaluestore.KeyValueStoreFactory
//import io.reactivex.schedulers.Schedulers
//import org.junit.Rule
//import org.junit.Test
//import org.junit.rules.TemporaryFolder
//
//class GsonConverterTest {
//    @Rule @JvmField val tempDir = TemporaryFolder().apply { create() }
//
//    @Test fun convertValue() {
//        val store = KeyValueStoreFactory.build().value<TestData>(tempDir.newFile(), GsonConverter(), TestData::class.java)
//        assertThat(store.blockingGet()).isNull()
//
//        store.put(TestData("1", 1), Schedulers.trampoline())
//        assertThat(store.blockingGet()).isEqualTo(TestData("1", 1))
//    }
//
//    @Test fun convertList() {
//        val store = KeyValueStore.list<TestData>(tempDir.newFile(), GsonConverter(), TestData::class.java)
//        assertThat(store.blockingGet()).isEmpty()
//
//        val list = listOf(TestData("1", 1), TestData("2", 2))
//        store.put(list, Schedulers.trampoline())
//        assertThat(store.blockingGet()).containsExactly(TestData("1", 1), TestData("2", 2))
//    }
//
//    data class TestData(val string: String, val integer: Int)
//}