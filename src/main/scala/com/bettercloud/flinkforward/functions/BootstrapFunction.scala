package com.bettercloud.flinkforward.functions

import com.bettercloud.flinkforward.Job._
import com.bettercloud.flinkforward.models.{ControlEvent, CustomerEvent, FilteredEvent}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.io.Source


class BootstrapFunction extends FlatMapFunction[ControlEvent, FilteredEvent] {
  override def flatMap(value: ControlEvent, out: Collector[FilteredEvent]): Unit = {
    val stream = getClass.getResourceAsStream("/events.txt")

    Source.fromInputStream(stream)
      .getLines
      .toList
      .map(x => CustomerEvent(x))
      .filter(x => x.customerId == value.bootstrapCustomerId)
      .foreach(x => {
        out.collect(FilteredEvent(x, List(value)))
      })
  }
}
