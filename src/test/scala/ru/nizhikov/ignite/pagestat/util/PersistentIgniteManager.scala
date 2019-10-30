package ru.nizhikov.ignite.pagestat.util

import java.nio.ByteBuffer

import org.apache.ignite.Ignition
import org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE
import org.apache.ignite.configuration.{DataStorageConfiguration, IgniteConfiguration}
import org.apache.ignite.internal.IgniteEx
import org.apache.ignite.internal.pagemem.{FullPageId, PageIdAllocator}
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx
import org.apache.ignite.internal.processors.cache.persistence.tree.io.{DataPageIO, PageIO}
import org.apache.ignite.internal.processors.cache.{GridCacheSharedContext, GridCacheUtils}

case class PersistentIgniteManager(igniteEx: IgniteEx, pageMemoryEx: PageMemoryEx) {
    // Used caches, needed for destroy when cleaning up
    private var caches: List[String] = List.empty
    // PageIOs map
    private var ios: Map[FullPageId, PageIO] = Map.empty

    def initPage(cacheName: String, pageType: Int, pageVer: Int): FullPageId = {
        igniteEx.getOrCreateCache(cacheName)
        caches ::= cacheName

        val cacheId = GridCacheUtils.cacheId(cacheName)
        val pageId = pageMemoryEx.allocatePage(cacheId, 0, PageIdAllocator.FLAG_DATA)
        val fullId = new FullPageId(pageId, cacheId)

        val io = PageIO.getPageIO(pageType, pageVer).asInstanceOf[PageIO]

        ios += fullId -> io

        acquireAndReleasePageForAction(fullId) {
            pageAddr =>
                io.initNewPage(
                    pageAddr,
                    fullId.pageId(),
                    pageMemoryEx.pageSize())
        }

        fullId
    }

    def writeRowsToDataPage(fullId: FullPageId, rows: List[Array[Byte]]): Option[Unit] =
        for {
            io <- ios.get(fullId)
                dataPageIo = io.asInstanceOf[DataPageIO]
        } yield
            rows.foreach {
                row =>
                    acquireAndReleasePageForAction(fullId) {
                        pageAddr: Long =>
                            dataPageIo.addRow(
                                pageAddr,
                                row,
                                pageMemoryEx.pageSize())
                    }
            }

    def dataPageFreeSpace(fullId: FullPageId): Option[Int] =
        for {
            io <- ios.get(fullId)
                dataPageIo = io.asInstanceOf[DataPageIO]
        } yield
            acquireAndReleasePageForAction(fullId) {
                pageAddr => dataPageIo.getFreeSpace(pageAddr)
            }

    def getPageBuffer(fullId: FullPageId): Option[ByteBuffer] =
        for {
            io <- ios.get(fullId)
                pageBuffer = acquireAndReleasePageForAction(fullId)(pageMemoryEx.pageBuffer)
        } yield
            pageBuffer

    def stop(destroyCaches: Boolean): Unit = {
        if (destroyCaches)
            caches.foreach(igniteEx.destroyCache)

        igniteEx.close()
    }

    private def acquireAndReleasePageForAction[R](fullId: FullPageId)(action: Long => R): R = {
        val page = pageMemoryEx.acquirePage(fullId.groupId(), fullId.pageId())
        try
            action(page)
        finally
            pageMemoryEx.releasePage(
                fullId.groupId(),
                fullId.pageId(),
                page)
    }
}

object PersistentIgniteManager {
    def apply(pageSz: Int = DFLT_PAGE_SIZE): PersistentIgniteManager = {
        val storageCfg = new DataStorageConfiguration().setPageSize(pageSz)
        storageCfg.getDefaultDataRegionConfiguration
            .setPersistenceEnabled(true)
        val cfg = new IgniteConfiguration().setDataStorageConfiguration(storageCfg)

        val igniteEx = Ignition.start(cfg)
            .asInstanceOf[IgniteEx]
        igniteEx.cluster().active(true)

        val sharedCtx: GridCacheSharedContext[AnyRef, AnyRef] = igniteEx.context()
            .cache()
            .context()

        val pageMemoryEx = sharedCtx.database()
            .dataRegion(null)
            .pageMemory()
            .asInstanceOf[PageMemoryEx]

        new PersistentIgniteManager(igniteEx, pageMemoryEx)
    }
}
