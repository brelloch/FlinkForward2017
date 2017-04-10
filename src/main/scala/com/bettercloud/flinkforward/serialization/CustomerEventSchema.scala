package com.bettercloud.flinkforward.serialization

import com.bettercloud.flinkforward.models.CustomerEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

import scala.util.control.NonFatal

class CustomerEventSchema extends DeserializationSchema[Option[CustomerEvent]] with SerializationSchema[CustomerEvent]{
  override def isEndOfStream(nextElement: Option[CustomerEvent]): Boolean = {
    false
  }

  override def deserialize(message: Array[Byte]): Option[CustomerEvent] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val jsonString = new String(message, "UTF-8")

    try {
      val obj: CustomerEvent = CustomerEvent.fromJson(jsonString)
      Some(obj)
    } catch {
      case NonFatal(ex) => println(ex); None
    }
  }

  override def serialize(element: CustomerEvent): Array[Byte] = {
    CustomerEvent.toJson(element).map(_.toByte).toArray
  }

  override def getProducedType: TypeInformation[Option[CustomerEvent]] = {
    BasicTypeInfo.getInfoFor(classOf[Option[CustomerEvent]])
  }
}
