package com.bettercloud.flinkforward.models

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag

trait ObjectMapperTrait[T] {

  /**
    * The Jackson ObjectMapper instance to use with Alerts
    */
  protected val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  /**
    * Serializes the provided Alert to a JSON string
    *
    * @param clazz the class to serialize
    * @return a JSON string
    */
  def toJson(clazz: T): String = {
    mapper.writeValueAsString(clazz)
  }

  /**
    * Deserializes the provided JSON string to an Alert
    *
    * @param json the JSON string to deserialize
    * @return an Alert
    */
  def fromJson(json: String)(implicit tag: ClassTag[T]): T = {
    mapper.readValue(json, tag.runtimeClass.asInstanceOf[Class[T]])
  }

}
