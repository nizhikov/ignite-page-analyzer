package ru.nizhikov.ignite

import java.io.File
import java.nio.ByteBuffer

import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.{COMMON_HEADER_END, T_CACHE_ID_AWARE_DATA_REF_LEAF, T_DATA,
    T_H2_EX_REF_INNER_END, T_H2_EX_REF_INNER_START, T_H2_EX_REF_LEAF_END, T_H2_EX_REF_LEAF_START, T_PAGE_LIST_NODE}
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
        private val BPLUS_ITEMS_OFF: Int = COMMON_HEADER_END + 2 + 8 + 8

        private def takeShort(offset: Int): Int = bb.getShort(offset) & 0xFFFF

        def pageType: Int = PageIO.getType(bb)

        /**
         * Gets free space by page type. If there are no handler methods to determine page type result would be [[None]]
         *
         * ===B+ pages===
         * For B+ pages item size, count, and max count in page are necessary.
         *
         * ====Items sizes====
         * Item sizes could be obtained according to respective constructors:
         *
         *  - for [[org.apache.ignite.internal.processors.cache.tree.CacheIdAwareDataLeafIO]] - 16 bytes.
         *  - for extras pages [[org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO]] and
         * [[org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO]] - 8 bytes + payloadSize.
         *
         * Payload size could be obtained in corresponding #register methods, in ''Apache Ignite 2.5.8-p4'' they would be as following:
         *
         *  - [[org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO]]#register
         * {{{
         *      public static void register() {
         *          for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++)
         *              PageIO.registerH2ExtraLeaf(getVersions((short)(PageIO.T_H2_EX_REF_LEAF_START + payload - 1), payload));
         *      }
         * }}}
         *  - for [[org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO]]#register:
         * {{{
         *      public static void register() {
         *          for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++)
         *              PageIO.registerH2ExtraInner(getVersions((short)(PageIO.T_H2_EX_REF_INNER_START + payload - 1), payload));
         *      }
         * }}}
         *
         * So, item size is equal to `pType - T_H2_EX_REF_LEAF_START + 9` for ''H2ExtrasLeafIO'' and `pType - T_H2_EX_REF_INNER_START + 9`
         * for ''H2ExtrasInnerIO''.
         *
         * ====Count====
         * Count for all B+ pages is calculated as one in [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO]]#getCount
         * method:
         * {{{
         *      public final int getCount(long pageAddr) {
         *          int cnt = PageUtils.getShort(pageAddr, CNT_OFF) & 0xFFFF;
         *
         *          assert cnt >= 0: cnt;
         *
         *          return cnt;
         *      }
         * }}}
         *
         * ====Max count====
         * Max count is determined as in #getMaxCount method of corresponding ancestor:
         *
         *  - for ''CacheIdAwareDataLeafIO'' and ''H2ExtrasLeafIO'' as in [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO]]#getMaxCount
         *  - for ''H2ExtrasInnerIO'' as in [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO]]#getMaxCount
         *
         * ====Free space====
         * Free space calculation leans to corresponding store and remove methods, which depend on item offset calculation,
         * so it would be different for [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO]]
         * and [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO]] ancestors.
         *
         * @param pType page type
         * @return free space optional
         */
        def pageFreeSpace(pType: Int): Option[Int] = pType match {
            // See AbstractDataPageIO#FREE_SPACE_OFF
            case T_DATA => Some(takeShort(COMMON_HEADER_END + 8))

            case T_PAGE_LIST_NODE =>
                Some(pageListNodeFreeSpace)

            case T_CACHE_ID_AWARE_DATA_REF_LEAF =>
                Some(bPlusLeafFreeSpace(16))

            case t if (t >= T_H2_EX_REF_LEAF_START) && (t <= T_H2_EX_REF_LEAF_END) =>
                Some(bPlusLeafFreeSpace(t - T_H2_EX_REF_LEAF_START + 9))

            case t if (t >= T_H2_EX_REF_INNER_START) && (t <= T_H2_EX_REF_INNER_END) =>
                Some(bPlusInnerFreeSpace(t - T_H2_EX_REF_INNER_START + 9))

            case _ => None
        }

        def pageListNodeFreeSpace: Int = {
            // See PagesListNodeIO#CNT_OFF
            val CNT_OFF = COMMON_HEADER_END + 8 + 8
            val PAGE_IDS_OFF = CNT_OFF + 2

            val cnt = takeShort(CNT_OFF)

            // See PagesListNodeIO#PAGE_IDS_OFF
            val capacity = (bb.capacity() - PAGE_IDS_OFF) >>> 3

            (capacity - cnt) * 8
        }

        /**
         * Method for B+ leaf pages max count calculation.
         * Max count of items in [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO]]#getMaxCount is determined as:
         * {{{
         *      @Override public int getMaxCount(long pageAddr, int pageSize) {
         *          return (pageSize - ITEMS_OFF) / getItemSize();
         *      }
         * }}}
         *
         * @param itemSz Size of item
         * @return Max count of items in B+ leaf page for a given item size
         */
        def bPlusLeafMaxCnt(itemSz: Int): Int = (bb.capacity() - BPLUS_ITEMS_OFF) / itemSz

        /**
         * Method for B+ inner pages max count calculation.
         * Max count of items in [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO]]#getMaxCount is determined as:
         * {{{
         *      @Override public int getMaxCount(long pageAddr, int pageSize) {
         *          // The structure of the page is the following:
         *          // |ITEMS_OFF|w|A|x|B|y|C|z|
         *          // where capital letters are data items, lowercase letters are 8 byte page references.
         *          return (pageSize - ITEMS_OFF - 8) / (getItemSize() + 8);
         *      }
         * }}}
         *
         * @param itemSz Size of item
         * @return Max count of items in B+ inner page for a given item size
         */
        def bPlusInnerMaxCnt(itemSz: Int): Int = (bb.capacity() - BPLUS_ITEMS_OFF - 8) / (itemSz + 8)

        /**
         * Method for B+ leaf pages free space calculation.
         * Offset in [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO]]#offset is determined as:
         * {{{
                @Override public final int offset(int idx) {
                    assert idx >= 0: idx;

                    return ITEMS_OFF + idx * getItemSize();
                }
         * }}}
         *
         * Methods for storing and removing items use this offset, so total free space for page could be determined as multiplication
         * of unused count of items to item size
         *
         * @param itemSz size of item
         * @param cnt current count of items, should not be changed by default
         * @param maxCntFun function to calculate max size, should not be changed by default
         * @return free space in bytes for page
         */
        def bPlusLeafFreeSpace(itemSz: Int, cnt: Int = takeShort(COMMON_HEADER_END), maxCntFun: Int => Int = bPlusLeafMaxCnt): Int =
            (maxCntFun(itemSz) - cnt) * itemSz

        /**
         * Method for B+ inner pages free space calculation.
         * Offset in [[org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO]]#offset is determined as:
         * {{{
         *       private int offset0(int idx, int shift) {
         *           return shift + (8 + getItemSize()) * idx;
         *        }
         *
         *       @Override public final int offset(int idx) {
         *           return offset0(idx, SHIFT_LINK);
         *      }
         * }}}
         *
         * Methods for storing and removing items use this offset, so total free space for page can be determined as
         * multiplication of unused count of items to '''item size plus 8'''.
         * ''This extra 8 bytes are not included in item size!''
         *
         * @param itemSz size of item
         * @param cnt current count of items, should not be changed by default
         * @param maxCntFun function to calculate max size, should not be changed by default
         * @return free space in bytes for page
         */
        def bPlusInnerFreeSpace(itemSz: Int, cnt: Int = takeShort(COMMON_HEADER_END), maxCntFun: Int => Int = bPlusInnerMaxCnt): Int =
            (maxCntFun(itemSz) - cnt) * (itemSz + 8)
    }
}
