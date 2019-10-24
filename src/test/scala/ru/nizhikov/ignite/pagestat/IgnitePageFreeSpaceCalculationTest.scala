package ru.nizhikov.ignite.pagestat

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.{COMMON_HEADER_END, T_CACHE_ID_AWARE_DATA_REF_LEAF, T_DATA,
    T_H2_EX_REF_INNER_END, T_H2_EX_REF_INNER_START, T_H2_EX_REF_LEAF_END, T_H2_EX_REF_LEAF_START, T_PAGE_LIST_NODE}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

/**
 * Simple test providing checks of [[IgnitePage()]]#getFreeSpace calculation for supported by page-analyzer page types
 */
class IgnitePageFreeSpaceCalculationTest extends FlatSpec with BeforeAndAfterEach {
    private val TEST_PAGE_SIZE = DFLT_PAGE_SIZE
    private val BPLUS_ITEMS_OFF: Int = COMMON_HEADER_END + 2 + 8 + 8

    // Test values for pages
    private val DATA_PAGE_TEST_FREE_SPACE: Int = 42
    private val PAGES_LIST_NODE_TEST_COUNT: Int = 77
    private val CACHE_ID_AWARE_DATA_LEAF_TEST_COUNT: Int = 100
    private val H2_EXTRAS_LEAF_TEST_PAYLOAD: Int = 32
    private val H2_EXTRAS_LEAF_TEST_COUNT: Int = 10

    // Fake buffer, acts as mock page buffer
    var fakePageBuffer: ByteBuffer = ByteBuffer.allocate(TEST_PAGE_SIZE)
        .order(ByteOrder.nativeOrder())

    override def beforeEach(): Unit = {

    }

    override def afterEach() {
        fakePageBuffer.clear()
    }

    /**
     * Check data pages
     */
    behavior of "Pages of type T_DATA=1 (AbstractDataPageIO)"

    it should "provide correct free space value" in {
        //Prepare page buffer
        fakePageBuffer.putShort(COMMON_HEADER_END + 8, DATA_PAGE_TEST_FREE_SPACE.toShort)

        val freeSpace = fakePageBuffer.pageFreeSpace(T_DATA)
            .getOrElse(0)
        assert(DATA_PAGE_TEST_FREE_SPACE === freeSpace)
    }

    /**
     * Check FreeList pages
     */
    behavior of "Pages of type T_PAGE_LIST_NODE=13 (PagesListNodeIO)"

    it should "provide correct free space value" in {
        //Prepare page buffer
        fakePageBuffer.putShort(COMMON_HEADER_END + 8 + 8, PAGES_LIST_NODE_TEST_COUNT.toShort)

        val freeSpace = fakePageBuffer.pageFreeSpace(T_PAGE_LIST_NODE)
            .getOrElse(0)
        assert(expectedPageListNodeFreeSpace === freeSpace)
    }

    /**
     * Check pages, processed by CacheIdAwareDataLeafIO
     */
    behavior of "Page of type T_CACHE_ID_AWARE_DATA_REF_LEAF=17 (CacheIdAwareDataLeafIO)"

    it should "provide correct free space value" in {
        //Prepare page buffer
        fakePageBuffer.putShort(COMMON_HEADER_END, CACHE_ID_AWARE_DATA_LEAF_TEST_COUNT.toShort)

        val freeSpace = fakePageBuffer.pageFreeSpace(T_CACHE_ID_AWARE_DATA_REF_LEAF)
            .getOrElse(0)
        val expectedFreeSpace = expectedBPlusLeafFreeSpace(16, CACHE_ID_AWARE_DATA_LEAF_TEST_COUNT)

        assert(expectedFreeSpace === freeSpace)
    }

    /**
     * Check pages, processed by H2ExtrasLeafIO
     */
    behavior of "H2 extras leaf page with type in range [10000...12047]"

    it should "provide correct free space value" in {
        //Prepare page buffer
        fakePageBuffer.putShort(COMMON_HEADER_END, H2_EXTRAS_LEAF_TEST_COUNT.toShort)

        val itemSz = H2_EXTRAS_LEAF_TEST_PAYLOAD + 8
        val pageType = T_H2_EX_REF_LEAF_START + H2_EXTRAS_LEAF_TEST_PAYLOAD - 1

        val expectedFreeSpace = expectedBPlusLeafFreeSpace(itemSz, H2_EXTRAS_LEAF_TEST_COUNT)
        val freeSpace = fakePageBuffer.pageFreeSpace(pageType)
            .getOrElse(0)

        assert(expectedFreeSpace === freeSpace)
    }

    // Calculate free space test value for PagesListNodeIO
    private def expectedPageListNodeFreeSpace: Int = {
        val PAGE_IDS_OFF = COMMON_HEADER_END + 8 + 8 + 2
        val cnt = PAGES_LIST_NODE_TEST_COUNT
        val capacity = (TEST_PAGE_SIZE - PAGE_IDS_OFF) >>> 3

        (capacity - cnt) * 8
    }

    // Calculate BPlusLeafIO free space
    private def expectedBPlusLeafFreeSpace(itemSz: Int, cnt: Int, maxCntFun: Int => Int = bPlusLeafMaxCnt): Int =
        (maxCntFun(itemSz) - cnt) * itemSz

    // Calculate BPlusLeafIO max count
    private def bPlusLeafMaxCnt(itemSz: Int): Int = (TEST_PAGE_SIZE - BPLUS_ITEMS_OFF) / itemSz
}
