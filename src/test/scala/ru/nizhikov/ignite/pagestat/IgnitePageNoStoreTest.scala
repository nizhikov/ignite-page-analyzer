package ru.nizhikov.ignite.pagestat

import org.apache.ignite.configuration.DataRegionConfiguration
import org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl
import org.apache.ignite.internal.pagemem.{FullPageId, PageIdAllocator, PageMemory}
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.MIN_DATA_PAGE_OVERHEAD
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO._
import org.apache.ignite.internal.processors.cache.persistence.tree.io.{AbstractDataPageIO, DataPageIO}
import org.apache.ignite.internal.util.IgniteUtils
import org.apache.ignite.logger.NullLogger
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen}

/**
 * Simple test providing checks of [[IgnitePage()]]#getFreeSpace calculation for supported by page-analyzer page types.
 * Utilizes [[PageMemoryNoStoreImpl]] to acquire and allocate pages.
 */
class IgnitePageNoStoreTest extends FlatSpec with BeforeAndAfterEach with GivenWhenThen {
    /** Taking into account free space calculation in [[AbstractDataPageIO]]#freeSpace reserved size for page
     * costists of ITEM_SIZE(2) + PAYLOAD_LEN_SIZE(8) + LINK_SIZE(2) */
    private val FREE_RESERVED_SIZE = 2 + 8 + 2

    /** Data page IO, used to work with data pages */
    val dataPageIo = DataPageIO.VERSIONS.forVersion(1)

    /** Logger, used by page memory */
    var log = new NullLogger()
    //    var log = new Log4JLogger(Logger.getLogger(this.getClass))

    /** Page memory for operating with data pages */
    var pageMemory: PageMemory = _

    override def beforeEach(): Unit = {
        pageMemory = memory();
        pageMemory.start()

        super.beforeEach()
    }

    override def afterEach() {
        try
            super.afterEach()
        finally
            pageMemory.stop(true)
    }

    /**
     * Check data pages
     */
    behavior of "Free space value for DataPageIO"

    it should "should be correct when page just initialized" in {
        Given("new empty data page")
        val id = createAndFillDataPage()

        When("obtaining free space of empty data page by means of PageAnalyzer and by means of DataPageIO")
        val expectedFree = dataPageFreeSpaceFromIo(id)

        val actualFree = dataPageFreeSpaceFromBuffer(id)

        Then(s"free space should be less than full page size ($DFLT_PAGE_SIZE bytes) to overhead amount (" +
            s"${MIN_DATA_PAGE_OVERHEAD} bytes)")
        assert(actualFree === DFLT_PAGE_SIZE - MIN_DATA_PAGE_OVERHEAD)

        And("obtained values of free space should be equal")
        assert(actualFree === expectedFree)
    }

    it should "should be zero when page is full" in {
        Given("initialized new data page")
        val id = createAndFillDataPage(new Array[Byte](DFLT_PAGE_SIZE - MIN_DATA_PAGE_OVERHEAD))

        When("obtaining free space of full data page by means of PageAnalyzer and by means of DataPageIO")
        val expectedFree = dataPageFreeSpaceFromIo(id)

        val actualFree = dataPageFreeSpaceFromBuffer(id)

        Then(s"page free space should be zero")
        assert(actualFree === 0)

        And("obtained values of free space should be equal")
        assert(actualFree === expectedFree)
    }

    it should "should be correct for single payload write" in {
        val payloadSz = 600
        Given(s"page filled with payload of $payloadSz bytes")
        val id = createAndFillDataPage(new Array[Byte](payloadSz))

        When("obtaining free space of this page")
        val expectedFree = dataPageFreeSpaceFromIo(id)

        val actualFree = dataPageFreeSpaceFromBuffer(id)

        Then(s"obtained value by means of PageAnalyzer should be equal to one obtained by means of DataPageIO")
        assert(actualFree === expectedFree)
    }

    /**
     * Calculate free space by means of [[IgnitePage()]] taking into account calculation of free space in
     * [[AbstractDataPageIO]]#getFreeSpace, where value, obtained from page header if decremented to reserved size of
     * bytes
     *
     * @param id [[FullPageId]] of desired page
     * @return free space in page
     */
    private def dataPageFreeSpaceFromBuffer(id: FullPageId) = {
        val buffer = acquireAndReleasePageForAction(id)(pageMemory.pageBuffer)
        val actualFree = buffer.pageFreeSpace(T_DATA).get - FREE_RESERVED_SIZE

        if (actualFree < 0)
            0
        else
            actualFree
    }

    /**
     * Initialize page memory: initialize logger, prepare temporary work dir for page memory mapped file,
     * create new [[PageMemoryNoStoreImpl]] and starts it.
     */
    private def memory(): PageMemory = {
        val drSz = 512L * 4096
        val drCfg = new DataRegionConfiguration().setMaxSize(drSz).setInitialSize(drSz)

        val memDir = IgniteUtils.resolveWorkDirectory(IgniteUtils.defaultWorkDirectory(), "pagemem", false);

        new PageMemoryNoStoreImpl(
            log,
            new MappedFileMemoryProvider(log, memDir),
            null,
            DFLT_PAGE_SIZE,
            drCfg,
            new DataRegionMetricsImpl(drCfg),
            false)
    }

    /**
     * Utility method for wrapping given action into page acquire-release sequence.
     *
     * @param fullId [[FullPageId]] of affected page.
     * @param action Action to do, should process page pointer (long).
     * @tparam R Type of value returned by action.
     * @return Result of action.
     */
    private def acquireAndReleasePageForAction[R](fullId: FullPageId)(action: Long => R): R = {
        val page = pageMemory.acquirePage(fullId.groupId(), fullId.pageId())
        try
            action(page)
        finally
            pageMemory.releasePage(fullId.groupId(), fullId.pageId(), page)
    }

    /**
     * Get data page size by means of [[DataPageIO]]
     *
     * @param fullId [[FullPageId]] of desired page.
     * @return Size of page.
     */
    def dataPageFreeSpaceFromIo(fullId: FullPageId): Int = acquireAndReleasePageForAction(fullId) {
        pageAddr => dataPageIo.getFreeSpace(pageAddr)
    }

    /**
     * Allocate and acquire page, init it with [[DataPageIO]] and add row with a given payload.
     *
     * @param payload If present, byte array array of payload, otherwise zero length array.
     * @return [[FullPageId]] of created page
     */
    private def createAndFillDataPage(payload: Array[Byte] = Array()): FullPageId = {
        val grpId = -1
        val fullId = new FullPageId(pageMemory.allocatePage(grpId, 1, PageIdAllocator.FLAG_DATA), grpId)

        acquireAndReleasePageForAction(fullId) {
            pageAddr => {
                dataPageIo.initNewPage(pageAddr, fullId.pageId(), pageMemory.pageSize())
                if (payload.length > 0)
                    dataPageIo.addRow(pageAddr, payload, pageMemory.pageSize())
            }
        }

        fullId
    }
}
