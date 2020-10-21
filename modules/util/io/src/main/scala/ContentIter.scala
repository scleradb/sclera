/**
* Sclera - IO
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.util.io

import java.net.{URL, MalformedURLException}
import java.nio.file.{Path, PathMatcher, FileSystem, FileSystems}
import java.io.{File, InputStream, FileInputStream}
import java.util.zip.{ZipInputStream, GZIPInputStream}

import scala.collection.mutable
import scala.util.{Try, Success, Failure}

/** Content of a file
 *
 * @param name File name
 * @param inputStream File's content as an InputStream
 */
class Content(val name: String, val inputStream: InputStream)

/** Recursively iterate over the files in a directory/zip file
  *
  * @param filterOpt If present, process only file names accepted by this filter
  */
class ContentIter(filterOpt: Option[String => Boolean]) {
    /** Accumulates input streams that need to be closed */
    private val streams: mutable.ListBuffer[InputStream] = mutable.ListBuffer()

    /** Contents of the root file/directory/URL */
    def iter(path: String): Iterator[Content] = Try(new URL(path)) match {
        case Success(url) => iter(url)
        case Failure(e: MalformedURLException) => iter(new File(path))
        case Failure(e) => throw e
    }

    /** Contents of the URL */
    def iter(url: URL): Iterator[Content] = url.getProtocol match {
        case "file" => iter(new File(url.toURI))
        case _ =>
            val urlis: InputStream = url.openStream()
            streams.append(urlis)
            iter(url.toString, urlis, isRoot = true)
    }

    /** Contents of the file/directory */
    def iter(f: File): Iterator[Content] = iter(f, isRoot = true)

    /** Contents of the file/directory */
    private def iter(f: File, isRoot: Boolean): Iterator[Content] =
        if( f.isDirectory ) {
            f.listFiles.iterator.flatMap(dirf => iter(dirf, isRoot = false))
        } else {
            val fis: FileInputStream = new FileInputStream(f)
            streams.append(fis)
            iter(f.getCanonicalPath, fis, isRoot)
        }

    /** Contents of the input stream */
    private def iter(
        name: String,
        is: InputStream,
        isRoot: Boolean
    ): Iterator[Content] =
        if( !isRoot && filterOpt.exists(filter => !filter(name)) ) {
            Iterator()
        } else if( name.endsWith(".zip") ) {
            val zis: ZipInputStream = new ZipInputStream(is)
            unzipIter(zis)
        } else if( name.endsWith(".gz") ) {
            val gzis: GZIPInputStream = new GZIPInputStream(is)
            iter(name.substring(0, name.length - 3), gzis, isRoot)
        } else Iterator(new Content(name, is))

    /** Contents of the zipped input stream */
    def unzipIter(zis: ZipInputStream): Iterator[Content] =
        Option(zis.getNextEntry) match {
            case Some(zipEntry) if( zipEntry.isDirectory ) => unzipIter(zis)
            case Some(zipEntry) =>
                iter(zipEntry.getName, zis, isRoot = false) ++ unzipIter(zis)
            case None => Iterator()
        }

    /** Close accumulated input streams */
    def close(): Unit = {
        streams.foreach { is => is.close() }
        streams.clear()
    }
}

object ContentIter {
    def apply(filterOpt: Option[String => Boolean] = None): ContentIter =
        new ContentIter(filterOpt)

    def apply(patterns: List[String]): ContentIter = apply(filterOpt(patterns))

    /** File name filter that accepts if name matches any of the patterns */
    private def filterOpt(patterns: List[String]): Option[String => Boolean] =
        if( patterns.isEmpty ) None else {
            val fs: FileSystem = FileSystems.getDefault()
            val matchers: List[PathMatcher] =
                patterns.map { pattern => fs.getPathMatcher(s"glob:$pattern") }
            Some(path => matchers.exists { m => m.matches(Path.of(path)) })
        }
}
