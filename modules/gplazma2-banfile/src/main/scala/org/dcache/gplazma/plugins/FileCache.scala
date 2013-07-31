package org.dcache.gplazma.plugins

import scala.collection.mutable
import java.io.File

trait FileCache[T] {

  private var cache = new mutable.HashMap[String, (Long, T)]()

  def getOrFetch(filename : String)(fetch : (String) => T) : T = {
    val file = new File(filename)
    cache.get(filename) match {
      case None => cache += (filename -> (file.lastModified, fetch(filename)))
      case Some((lastFetch, _)) if file.lastModified > lastFetch => cache(filename) = (file.lastModified, fetch(filename))
      case _ => // entry exists and is up to date
    }
    cache(filename)._2
  }

}
