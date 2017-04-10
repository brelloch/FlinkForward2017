package com.bettercloud.flinkforward.functions

import com.bettercloud.flinkforward.models.{ControlEvent, QualifiedEvent}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector


class CounterFunction extends FlatMapFunction[QualifiedEvent, ControlEvent] {
  var counts = scala.collection.mutable.HashMap[String, Int]()

  override def flatMap(value: QualifiedEvent, out: Collector[ControlEvent]): Unit = {
    val key = s"${value.event.customerId}${value.control.alertId}"
    if (counts.contains(key)) {
      counts.put(key, counts.get(key).get + 1)
      println(s"Count for ${key}: ${counts.get(key).get}")
    } else {
      val c = value.control
      counts.put(key, 1)
      out.collect(ControlEvent(c.customerId, c.alertId, c.alertName, c.alertDescription, c.threshold, c.jsonPath, value.event.customerId))
      println(s"Bootstrap count for ${key}: ${counts.get(key).get}")
    }
  }
}