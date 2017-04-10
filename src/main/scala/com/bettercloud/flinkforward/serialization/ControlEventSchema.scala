package com.bettercloud.flinkforward.serialization

import com.bettercloud.flinkforward.models.ControlEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

import scala.util.control.NonFatal

/**
  * Created by brelloch on 3/20/17.
  */
class ControlEventSchema extends DeserializationSchema[Option[ControlEvent]] with SerializationSchema[ControlEvent]{
  override def isEndOfStream(nextElement: Option[ControlEvent]): Boolean = {
    false
  }

  override def deserialize(message: Array[Byte]): Option[ControlEvent] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val jsonString = new String(message, "UTF-8")

    try {
      val obj: ControlEvent = ControlEvent.fromJson(jsonString)
      Some(obj)
    } catch {
      case NonFatal(ex) => None
    }
  }

  override def serialize(element: ControlEvent): Array[Byte] = {
    ControlEvent.toJson(element).map(_.toByte).toArray
  }

  override def getProducedType: TypeInformation[Option[ControlEvent]] = {
    BasicTypeInfo.getInfoFor(classOf[Option[ControlEvent]])
  }
}
