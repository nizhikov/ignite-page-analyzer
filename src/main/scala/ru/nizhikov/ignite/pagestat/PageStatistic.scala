package ru.nizhikov.ignite.pagestat

import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager._
import org.apache.log4j.Logger

import scala.collection.mutable

/**
 */
class PageStatistic(extLog: Boolean, pageSz: Int) {
    /** */
    val log = Logger.getLogger(this.getClass)

    /** */
    def collect(dir: String): Unit = {
        log.info(s"Starting analyze $dir")

        new File(dir)
            .listFiles
            .filter(_.isDirectory)
            .filterNot(d ⇒ d.getName == META_STORAGE_NAME || d.getName == "cp")
            .foreach(collectCacheInfo)
    }

    def collectCacheInfo(dir: File): Unit = {
        val cacheName = dir.getName
            .replace(CACHE_DIR_PREFIX, "")
            .replace(CACHE_GRP_DIR_PREFIX, "")

        log.info(s"Found cache[${cacheName}] directory $dir")

        val fullStat = mutable.Map[Int, Array[Long]]()

        dir.listFiles()
            .filter(f ⇒ f.isFile && (f.getName.startsWith(INDEX_FILE_NAME) || f.getName.startsWith(PART_FILE_PREFIX)))
            .flatMap(analyzePageFile(cacheName, _))
            .foreach(e ⇒ {
                if (!fullStat.contains(e._1))
                    fullStat.put(e._1, new Array[Long](2))

                for (i ← e._2.indices)
                    fullStat(e._1)(i) += e._2(i)
            })

        log.info(s"  *** FULLL STATISTICS FOR $cacheName ***")

        var fullSz: Long = 0
        var freeSz: Long = 0

        fullStat.toSeq
            .sortBy(_._1)
            .foreach(e ⇒ {
                val sz = size(e._2(1))
                val freePercent = (e._2(1)*100.0)/(e._2(0)*pageSz)

                fullSz = fullSz + e._2(0)
                freeSz = freeSz + e._2(1)

                log.info(f"${e._1}%4d -> [${e._2(0)}%6d pages, $sz free, $freePercent%5.2f%% unused]")
            })

        log.info("  ------------------")
        log.info(s"  Full size   ${size(fullSz*pageSz)}")
        log.info(s"  Free size   ${size(freeSz)}")
        log.info(f"  Free percent ${freeSz*100.0/(fullSz*pageSz)}%5.2f%%")
    }

    /**
     * @param cache Cache name.
     * @param part Partition file.
     * @return Tuple of 3 Long. (File size, page used space, page unused space). All numbers in bytes.
     */
    def analyzePageFile(cache: String, part: File): Map[Int, Array[Long]] = {
        if (extLog)
            log.info(s"Partition file. [name=${part.getName},size=${size(part.length())},cache=$cache]")

        closeAfter(FileChannel.open(part.toPath, StandardOpenOption.READ)) { ch ⇒
            val page = ByteBuffer.allocate(pageSz).order(ByteOrder.nativeOrder())

            var readed = ch.read(page)

            if (readed != pageSz) {
                log.warn(s"Can't read full page.[exp=$pageSz,read=$readed")

                return Map.empty
            }

            page.flip()

            //First page in the PageStore is header. Simply skipping it.
            readed = ch.read(page)

            //pagetType, (count, freespace)
            val stat = mutable.Map[Int, Array[Long]]()

            while(readed > 0) {
                if (readed != pageSz) {
                    log.warn(s"Can't read full page.[exp=$pageSz,read=$readed")

                    return Map.empty
                }

                page.flip()

                val pageType = page.pageType

                if (!stat.contains(pageType))
                    stat += pageType → new Array[Long](2)

                stat(pageType)(0) += 1
                stat(pageType)(1) += page
                    .pageFreeSpace(pageType)
                    .getOrElse(0)

                readed = ch.read(page)
            }

            stat.toSeq.sortBy(_._1).foreach(e ⇒ log.debug(e._1 + " -> " + e._2.mkString("[", ",", "]")))

            stat.toMap
        }
    }

    def size(bytesSz: Long): String = {
        val kb = bytesSz/1024.0

        if (kb < 2000)
            return f"$kb%6.2fK"

        val mb = kb/1024

        if (mb < 2000)
            return f"$mb%6.2fM"

        val gb = mb/1024

        f"$gb%6.2fG"
    }
}

object PageStatistic {
    def apply(extLog: Boolean, pageSz: Int = DFLT_PAGE_SIZE): PageStatistic = new PageStatistic(extLog, pageSz)
}
