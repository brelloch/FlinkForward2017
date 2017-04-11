package com.bettercloud.flinkforward.serialization

import com.bettercloud.flinkforward.models.CustomerEvent
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

import scala.util.{Failure, Success, Try}

class CustomerEventSchema extends DeserializationSchema[Option[CustomerEvent]] with SerializationSchema[CustomerEvent]{
  override def isEndOfStream(nextElement: Option[CustomerEvent]): Boolean = {
    false
  }

  override def deserialize(message: Array[Byte]): Option[CustomerEvent] = {
    val jsonString = new String(message, "UTF-8")

    Try(CustomerEvent.fromJson(jsonString)) match {
      case Success(customerEvent) => Some(customerEvent)
      case Failure(ex) => None
    }
  }

  override def serialize(element: CustomerEvent): Array[Byte] = {
    CustomerEvent.toJson(element).map(_.toByte).toArray
  }

  override def getProducedType: TypeInformation[Option[CustomerEvent]] = {
    BasicTypeInfo.getInfoFor(classOf[Option[CustomerEvent]])
  }
}
