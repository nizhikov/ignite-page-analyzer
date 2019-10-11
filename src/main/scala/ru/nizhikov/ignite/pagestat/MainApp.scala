/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.nizhikov.ignite.pagestat

import java.io.File

import org.apache.log4j.xml.DOMConfigurator
import ru.nizhikov.ignite.pagestat.Config._

/**
 */
object MainApp extends App {
    DOMConfigurator.configure(this.getClass.getResource("/log4j.xml"))

    val DEFAULT_POOL_SIZE = 2

    val parser = new scopt.OptionParser[Config]("java -jar ignite-page-analyzer.jar") {
        head("ignite-page-analyzer", "0.0.1")

        cmd(PAGE_STAT).action( (_, c) => c.copy(command = Some(PAGE_STAT)) ).
            text("Gather statistics about Ignite PDS pages.").
            children(
                opt[String]("dir").abbr("d").action((v, c) ⇒ c.copy(dir = Some(v)))
                    .text("Path to the Node PDS directory. Example \"/my/ignite/pds/node-000ffff0000/\""),
                opt[Unit]("verbose").abbr("v").action((_, c) ⇒ c.copy(extLog = true)).
                    text("Extended output"),
                opt[Unit]("index").abbr("i").action((_, c) ⇒ c.copy(checkIndexes = true)).
                    text("Check index.bin files instead of part*.bin")
            )
        help("help").text("prints this usage text")
        checkConfig { c =>
            c.command match {
                case Some(PAGE_STAT) ⇒
                    if (c.dir.isEmpty || !new File(c.dir.get).isDirectory)
                        failure(s"File ${c.dir.map(new File(_).getAbsoluteFile)} doesn't exists or not a directory.")
                    else
                        success
                case _ ⇒
                    failure("Unknown command")
            }
        }
    }

    parser.parse(args, Config()) match {
        case Some(config) => config.command match {
            case Some(PAGE_STAT) ⇒ PageStatistic(config.extLog, config.checkIndexes).collect(config.dir.get)
        }

        case _ =>
    }
}
