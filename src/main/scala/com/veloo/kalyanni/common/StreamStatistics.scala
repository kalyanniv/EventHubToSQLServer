package com.veloo.kalyanni.workloads

object StreamStatistics {

  val streamLengthKey: String = "StreamLength"

  val streamLength = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0L)(_ + _)
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }
}