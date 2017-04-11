package com.bettercloud.flinkforward.serialization

import com.bettercloud.flinkforward.models.ControlEvent
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

import scala.util.{Failure, Success, Try}

class ControlEventSchema extends DeserializationSchema[Option[ControlEvent]] with SerializationSchema[ControlEvent]{
  override def isEndOfStream(nextElement: Option[ControlEvent]): Boolean = {
    false
  }

  override def deserialize(message: Array[Byte]): Option[ControlEvent] = {
    val jsonString = new String(message, "UTF-8")

    Try(ControlEvent.fromJson(jsonString)) match {
      case Success(controlEvent) => Some(controlEvent)
      case Failure(ex) => None
    }

  }

  override def serialize(element: ControlEvent): Array[Byte] = {
    ControlEvent.toJson(element).map(_.toByte).toArray
  }

  override def getProducedType: TypeInformation[Option[ControlEvent]] = {
    BasicTypeInfo.getInfoFor(classOf[Option[ControlEvent]])
  }
}
