package com.bettercloud.flinkforward.functions

import com.bettercloud.flinkforward.Constants
import com.bettercloud.flinkforward.models.{ControlEvent, CustomerEvent, FilteredEvent}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class FilterFunction() extends RichCoFlatMapFunction[ControlEvent, CustomerEvent, FilteredEvent] {

  var configs = new mutable.ListBuffer[ControlEvent]()

  override def flatMap1(value: ControlEvent, out: Collector[FilteredEvent]): Unit = {
    configs = configs.filter(x => (x.customerId != value.customerId) && (x.alertId != value.alertId))
    configs.append(value)
  }

  override def flatMap2(value: CustomerEvent, out: Collector[FilteredEvent]): Unit = {
    val eventConfigs = configs.filter(x => (x.customerId == x.customerId) || (x.customerId == Constants.GLOBAL_CUSTOMER_ID))

    if (eventConfigs.size > 0) {
      out.collect(FilteredEvent(value, eventConfigs.toList))
    }
  }

}
