package ru.nizhikov.ignite.pagestat

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE
import org.apache.ignite.internal.pagemem.FullPageId
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.MIN_DATA_PAGE_OVERHEAD
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, GivenWhenThen}
import ru.nizhikov.ignite.pagestat.util.PersistentIgniteManager

import scala.util.Random

/**
 * Simple test providing checks of [[IgnitePage()]]#getFreeSpace calculation for supported by page-analyzer page types
 */
class IgnitePageFreeSpaceCalculationTest extends FlatSpec with BeforeAndAfterEach with BeforeAndAfterAll with GivenWhenThen {
    private val PAGE_SIZE = DFLT_PAGE_SIZE
    private val BPLUS_ITEMS_OFF: Int = COMMON_HEADER_END + 2 + 8 + 8
    private val CACHE_TEST_NAME: String = "cache"

    // Test values for pages
    // DataPageIO
    private val DATA_PAGE_TEST_FREE_SPACE: Int = 42
    private val DATA_PAGES_TESTS_AMOUNT = 10
    private val MAX_DATA_ROWS_AMOUNT = 3

    private val PAGES_LIST_NODE_TEST_COUNT: Int = 77

    private val CACHE_ID_AWARE_DATA_LEAF_TEST_COUNT: Int = 100

    private val H2_EXTRAS_LEAF_TEST_PAYLOAD: Int = 32
    private val H2_EXTRAS_LEAF_TEST_COUNT: Int = 10

    private val H2_EXTRAS_INNER_TEST_PAYLOAD: Int = 16
    private val H2_EXTRAS_INNER_TEST_COUNT: Int = 30

    // Fake buffer, acts as mock page buffer
    val fakePageBuffer: ByteBuffer = ByteBuffer.allocate(PAGE_SIZE)
        .order(ByteOrder.nativeOrder())

    var igniteManager: PersistentIgniteManager = _

    /**
     * Check data pages
     */
    behavior of "Page of type T_DATA=1 (AbstractDataPageIO)"

    it should "be initialized empty correctly" in {
        Given("initialized new data page")
        val id = igniteManager.initPage(CACHE_TEST_NAME, T_DATA, 1)

        When("getting free space of this page")
        val actualFree = igniteManager.dataPageFreeSpace(id)

        Then(s"it should be found with id = ${id}")
        assert(actualFree.isDefined)

        And(s"free space should be less than full page size = $PAGE_SIZE to overhead amount of bytes = ${MIN_DATA_PAGE_OVERHEAD}")
        assert(actualFree.get === PAGE_SIZE - MIN_DATA_PAGE_OVERHEAD)
    }

    it should "show zero free space when it is full" in {
        Given("initialized new data page")
        val id = igniteManager.initPage(CACHE_TEST_NAME, T_DATA, 1)

        When("getting free space of full data page")
        val bytes = new Array[Byte](PAGE_SIZE - MIN_DATA_PAGE_OVERHEAD)
        Random.nextBytes(bytes)
        igniteManager.writeRowsToDataPage(id, List(bytes))
        val actualFree = igniteManager.dataPageFreeSpace(id)

        Then(s"page should be found with it's id = ${id}")
        assert(actualFree.isDefined)

        And(s"page free space should be zero")
        assert(actualFree.get === 0)
    }

    it should "provide correct free space for random rows additions" in {
        Given(s"test pages amount = $DATA_PAGES_TESTS_AMOUNT, max test rows in each page = $MAX_DATA_ROWS_AMOUNT")
        val pageIds = fillDataPagesWithRandomRows(DATA_PAGES_TESTS_AMOUNT)

        // See AbstractDataPageIO#freeSpace
        val ITEM_SIZE = 2
        val PAYLOAD_LEN_SIZE = 8
        val LINK_SIZE = 2

        Then("page analyzer should obtain free space for all pages correctly")
        for {
            id <- pageIds
                expectedFree <- igniteManager.dataPageFreeSpace(id)
                buff <- igniteManager.getPageBuffer(id)
                free <- buff.pageFreeSpace(T_DATA)
                actualFree = free - (ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE)
        } assert(actualFree === expectedFree)
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

    /**
     * Check pages, processed by H2InnerLeafIO
     */
    behavior of "H2 inner leaf page with type in range [20000...22047]"

    it should "provide correct free space value" in {
        //Prepare page buffer
        fakePageBuffer.putShort(COMMON_HEADER_END, H2_EXTRAS_INNER_TEST_COUNT.toShort)

        val itemSz = H2_EXTRAS_INNER_TEST_PAYLOAD + 8
        val pageType = T_H2_EX_REF_LEAF_START + H2_EXTRAS_INNER_TEST_PAYLOAD - 1

        val expectedFreeSpace = expectedBPlusLeafFreeSpace(itemSz, H2_EXTRAS_INNER_TEST_COUNT)
        val freeSpace = fakePageBuffer.pageFreeSpace(pageType)
            .getOrElse(0)

        assert(expectedFreeSpace === freeSpace)
    }

    override def beforeAll(): Unit = {
        prepareAll()
        super.beforeAll()
    }

    override def afterAll(): Unit = {
        try super.afterAll()
        finally releaseAll()
    }

    override def beforeEach(): Unit = {
        super.beforeEach()
    }

    override def afterEach() {
        try super.afterEach()
        finally {
            fakePageBuffer.clear()
        }
    }

    private def prepareAll(): Unit = {
        // TODO setup logger

        igniteManager = PersistentIgniteManager(PAGE_SIZE)
    }

    private def releaseAll(): Unit = {
        igniteManager.stop(true)
    }

    // Calculate free space test value for PagesListNodeIO
    private def expectedPageListNodeFreeSpace: Int = {
        val PAGE_IDS_OFF = COMMON_HEADER_END + 8 + 8 + 2
        val cnt = PAGES_LIST_NODE_TEST_COUNT
        val capacity = (PAGE_SIZE - PAGE_IDS_OFF) >>> 3

        (capacity - cnt) * 8
    }

    //BPlusLeafIO

    //Calculate BPlusLeafIO free space
    private def expectedBPlusLeafFreeSpace(itemSz: Int, cnt: Int, maxCntFun: Int => Int = bPlusLeafMaxCnt): Int =
        (maxCntFun(itemSz) - cnt) * itemSz

    //Calculate BPlusLeafIO max count
    private def bPlusLeafMaxCnt(itemSz: Int): Int = (PAGE_SIZE - BPLUS_ITEMS_OFF) / itemSz

    //BPlusInnerIO

    //Calculate BPlusInnerIO free space
    private def bPlusInnerFreeSpace(itemSz: Int, cnt: Int, maxCntFun: Int => Int = bPlusInnerMaxCnt): Int =
        (maxCntFun(itemSz) - cnt) * (itemSz + 8)

    //Calculate BPlusInnerIO max count
    private def bPlusInnerMaxCnt(itemSz: Int): Int = (PAGE_SIZE - BPLUS_ITEMS_OFF - 8) / (itemSz + 8)

    // Fill given amount of pages
    private def fillDataPagesWithRandomRows(pagesAmount: Int): List[FullPageId] = {
        var idsList: List[FullPageId] = List.empty

        for (i <- 1 to pagesAmount) {
            val (free, rowsList) = createRandomRows(MAX_DATA_ROWS_AMOUNT)

            val fullId = igniteManager.initPage(CACHE_TEST_NAME, T_DATA, 1)
            assert(
                igniteManager.writeRowsToDataPage(fullId, rowsList)
                    .isDefined,
                "Page should be initialized!")
            When(s"created page #$i with id=$fullId")

            val freeSpaceOpt = igniteManager.dataPageFreeSpace(fullId)
            assert(freeSpaceOpt.isDefined, "Page should be initialized!")

            assert(free === igniteManager.dataPageFreeSpace(fullId).get)
            idsList ::= fullId
        }

        idsList
    }

    // Get tuple of free space and list of randomly created rows (bytes)
    private def createRandomRows(amount: Int = MAX_DATA_ROWS_AMOUNT): (Int, List[Array[Byte]]) = {
        // See AbstractDataPageIO#addRow and flags "SHOW_PAYLOAD_LEN | SHOW_ITEM"
        val LEN_INDEX_SIZE = 2 + 2

        // See AbstractDataPageIO#MIN_DATA_PAGE_OVERHEAD
        var free = PAGE_SIZE - MIN_DATA_PAGE_OVERHEAD
        var count = 0
        var bytesList: List[Array[Byte]] = List.empty

        while (count < amount && free > LEN_INDEX_SIZE) {
            val payloadSize = Random.nextInt(free - LEN_INDEX_SIZE) + 1
            val fullSize = payloadSize + LEN_INDEX_SIZE

            val bytes = new Array[Byte](payloadSize)
            Random.nextBytes(bytes)
            bytesList ::= bytes

            free -= fullSize
            count += 1
        }

        // Assert correct sizes calculation
        val calcSz = free +
            bytesList.map(_.length).sum +
            count * LEN_INDEX_SIZE +
            MIN_DATA_PAGE_OVERHEAD
        assert(calcSz === PAGE_SIZE)

        free -> bytesList
    }
}
