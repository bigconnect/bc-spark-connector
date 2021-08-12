package io.bigconnect.spark

import org.slf4j.Logger

object TestUtil {
  def closeSafety(autoCloseable: AutoCloseable, logger: Logger = null): Unit = {
    try {
      autoCloseable match {
        case null => Unit
        case _ => autoCloseable.close()
      }
    } catch {
      case t: Throwable => if (logger != null) {
        t.printStackTrace()
        logger.warn(s"Cannot close ${autoCloseable.getClass.getSimpleName} because of the following exception:", t)
      }
    }
  }
}
