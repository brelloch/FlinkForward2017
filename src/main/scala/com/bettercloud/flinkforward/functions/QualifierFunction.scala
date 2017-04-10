package com.bettercloud.flinkforward.functions

import com.bettercloud.flinkforward.models.{FilteredEvent, QualifiedEvent}
import com.jayway.jsonpath.JsonPath
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.util.Try


class QualifierFunction extends FlatMapFunction[FilteredEvent, QualifiedEvent] {
  override def flatMap(value: FilteredEvent, out: Collector[QualifiedEvent]): Unit = {
    value.controls.foreach(control => {
      Try {
        val parse = JsonPath.parse(value.event.payload) // move parse outside of foreach
        val result: String = parse.read(control.jsonPath)

        if (!result.isEmpty) {
          out.collect(QualifiedEvent(value.event, control))
        }
      }
    })
  }
}
