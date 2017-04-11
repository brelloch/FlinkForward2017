package com.bettercloud.flinkforward.functions

import com.bettercloud.flinkforward.models.{FilteredEvent, QualifiedEvent}
import com.jayway.jsonpath.JsonPath
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.util.Try


class QualifierFunction extends FlatMapFunction[FilteredEvent, QualifiedEvent] {
  override def flatMap(value: FilteredEvent, out: Collector[QualifiedEvent]): Unit = {
    Try(JsonPath.parse(value.event.payload)).map(ctx => {
      value.controls.foreach(control => {
        Try {
          val result: String = ctx.read(control.jsonPath)

          if (!result.isEmpty) {
            out.collect(QualifiedEvent(value.event, control))
          }
        }
      })
    })
  }
}
