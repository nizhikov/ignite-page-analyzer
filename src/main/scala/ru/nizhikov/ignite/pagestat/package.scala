package ru.nizhikov.ignite

import java.io.File
import java.nio.ByteBuffer

import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.COMMON_HEADER_END
import org.apache.log4j.Logger

/**
 */
package object pagestat {
    private val logger = Logger.getLogger(this.getClass)

    def filesList(dir: String): Seq[File] = {
        val d = new File(dir)
        if (d.exists() && d.isDirectory)
            d.listFiles.filter(_.isFile).sorted
        else
            Seq.empty
    }

    implicit class ExtendedMap(map: Map[String, String]) {
        def getOrNull(key: String) = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value
            else
                null
        }

        def getIntOrNull(key: String): java.lang.Integer = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value.toInt
            else
                null
        }

        def getLongOrNull(key: String): java.lang.Long = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value.toLong
            else
                null
        }

        def getDoubleOrNull(key: String): java.lang.Double = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value.toDouble
            else
                null
        }
    }

    object closeAfter {
        def apply[R <: AutoCloseable, T](r: R)(c: (R) ⇒ T) = {
            try {
                c(r)
            }
            finally {
                r.close
            }
        }
    }

    implicit class IgnitePage(bb: ByteBuffer) {
        def takeShort(offset: Int): Int = bb.getShort(offset) & 0xFFFF

        def pageType: Int = PageIO.getType(bb)

        def treePageFreeSpace(itemSz: Int) = {
            // See BPlusIO#ITEMS_OFF
            val ITEMS_OFF = COMMON_HEADER_END + 2 + 8 + 8
            val maxCnt = (bb.capacity() - ITEMS_OFF)/itemSz

            val cnt = bb.takeShort(COMMON_HEADER_END)

            (maxCnt - cnt)*itemSz
        }
    }
}
