package ru.nizhikov.ignite.pagestat

/**
 */
case class Config(
    command: Option[String] = None,
    dir: Option[String] = None
)

object Config {
    val PAGE_STAT = "page-statistics"
}
