package ru.nizhikov.ignite.pagestat

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.{COMMON_HEADER_END, T_DATA, T_PAGE_LIST_NODE}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

/**
 * Simple test providing checks of [[IgnitePage()]]#getFreeSpace calculation for supported by page-analyzer page types
 */
class IgnitePageFreeSpaceCalculationTest extends FlatSpec with BeforeAndAfterEach {
    private val TEST_PAGE_SIZE = DFLT_PAGE_SIZE

    // Test values for pages
    private val DATA_PAGE_TEST_FREE_SPACE: Int = 42
    private val PAGES_LIST_NODE_TEST_COUNT: Int = 77

    // Fake buffer, acts as mock page buffer
    var fakePageBuffer: ByteBuffer = ByteBuffer.allocate(TEST_PAGE_SIZE)
        .order(ByteOrder.nativeOrder())

    override def beforeEach(): Unit = {

    }

    override def afterEach() {
        fakePageBuffer.clear()
    }

    behavior of "Pages of type T_DATA=1 (AbstractDataPageIO)"

    it should "provide correct free space value" in {
        fakePageBuffer.putShort(COMMON_HEADER_END + 8, DATA_PAGE_TEST_FREE_SPACE.toShort)

        val freeSpace = fakePageBuffer.pageFreeSpace(T_DATA)
            .getOrElse(0)
        assert(freeSpace === DATA_PAGE_TEST_FREE_SPACE)
    }

    behavior of "Pages of type T_PAGE_LIST_NODE=13 (PagesListNodeIO)"

    it should "provide correct free space value" in {
        fakePageBuffer.putShort(COMMON_HEADER_END + 8 + 8, PAGES_LIST_NODE_TEST_COUNT.toShort)

        val freeSpace = fakePageBuffer.pageFreeSpace(T_PAGE_LIST_NODE)
            .getOrElse(0)
        assert(freeSpace === getTestPageListNodeFreeSpace)
    }


    // Calculate free space test value for PagesListNodeIO
    private def getTestPageListNodeFreeSpace: Int = {
        val PAGE_IDS_OFF = COMMON_HEADER_END + 8 + 8 + 2

        val cnt = PAGES_LIST_NODE_TEST_COUNT
        val capacity = (TEST_PAGE_SIZE - PAGE_IDS_OFF) >>> 3
        (capacity - cnt) * 8
    }
}
