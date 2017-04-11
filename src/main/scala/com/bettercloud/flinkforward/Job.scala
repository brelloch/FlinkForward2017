package com.bettercloud.flinkforward

import java.util.{Properties, UUID}

import com.bettercloud.flinkforward.functions.{BootstrapFunction, CounterFunction, FilterFunction, QualifierFunction}
import com.bettercloud.flinkforward.models.{ControlEvent, CustomerEvent, FilteredEvent}
import com.bettercloud.flinkforward.serialization.{ControlEventSchema, CustomerEventSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}


// Example test messages:
//
//  control:
//    {"customerId":"deadbeef-dead-beef-dead-beefdeadbeef","alertId":"f4b9d5a5-b5d3-41e1-ae8c-2e5d357fb83e","alertName":"Testing Alert","alertDescription":"This is a test alert.","threshold":2,"jsonPath":"$.store.book[0].author"}
//    {
//      "customerId":"deadbeef-dead-beef-dead-beefdeadbeef",
//      "alertId":"f4b9d5a5-b5d3-41e1-ae8c-2e5d357fb83e",
//      "alertName":"Testing Alert",
//      "alertDescription":"This is a test alert.",
//      "threshold":2,
//      "jsonPath":"$.store.book[0].author"
//    }
//  event that triggers:
//    {"customerId":"9f20f8a4-d8c8-4a74-ac90-3452b97c2e34","payload":"{\"store\": {\"book\": [{\"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95},{\"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99},{\"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99},{\"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"0-395-19395-8\",\"price\": 22.99}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}},\"expensive\": 10}"}
//    {
//      "customerId":"9f20f8a4-d8c8-4a74-ac90-3452b97c2e34",
//      "payload":"{\"store\": {\"book\": [{\"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95},{\"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99},{\"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99},{\"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"0-395-19395-8\",\"price\": 22.99}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}},\"expensive\": 10}"
//    }
//  events that do nothing:
//    {"customerId":"9f20f8a4-d8c8-4a74-ac90-3452b97c2e34","payload":""}
//    {"customerId":"9f20f8a4-d8c8-4a74-ac90-3452b97c2e34","payload":"{\"firstName\":\"John\",\"lastName\":\"doe\",\"age\":26,\"address\":{\"streetAddress\":\"naist street\",\"city\":\"Nara\",\"postalCode\":\"630-0192\"},\"phoneNumbers\":[{\"type\":\"iPhone\",\"number\":\"0123-4567-8888\"},{\"type\":\"home\",\"number\":\"0123-4567-8910\"}]}"}


object Job {

  def main(args: Array[String]) {
    // set up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val filterFunction = new FilterFunction
    val qualifierFunction = new QualifierFunction
    val counterFunction = new CounterFunction
    val bootstrapFunction = new BootstrapFunction

    val bootstrapStream = env.addSource(new FlinkKafkaConsumer09("bootstrap", new ControlEventSchema(), properties))
      .filter(x => x.isDefined)
      .map(x => x.get)
      .flatMap(bootstrapFunction).name("Bootstrap Function")
      .keyBy((fe: FilteredEvent) => { fe.event.customerId } )

    val bootstrapSink = new FlinkKafkaProducer09("bootstrap", new ControlEventSchema(), properties)

    val eventStream = env.addSource(new FlinkKafkaConsumer09("events", new CustomerEventSchema(), properties))
      .filter(x => x.isDefined)
      .map(x => x.get)
      .keyBy((ce: CustomerEvent) => { ce.customerId } )

    // Grab "control" events from Kafka
    // Separate the streams into "global" which get broadcast to all tasks and "specific" which only impact a single customer
    val controlStream = env.addSource(new FlinkKafkaConsumer09("controls", new ControlEventSchema(), properties))
                           .filter(x => x.isDefined)
                           .map(x => x.get)
                           .name("Control Source")
                           .split((ce: ControlEvent) => {
                             ce.customerId match {
                               case Constants.GLOBAL_CUSTOMER_ID => List("global")
                               case _ => List("specific")
                             }
                           })

    // Broadcast "global" control messages
    val globalControlStream = controlStream.select("global").broadcast

    // Key "specific" messages by customerId
    val specificControlStream = controlStream.select("specific")
      .keyBy((ce: ControlEvent) => { ce.customerId })

    // Join the control and event streams
    val filterStream = globalControlStream.union(specificControlStream)
      .connect(
        eventStream
      )
      .flatMap(filterFunction).name("Filtering Function")
      .union(bootstrapStream)
      .flatMap(qualifierFunction).name("Qualifier Function")
      .flatMap(counterFunction).name("Counter Function")
      .addSink(bootstrapSink)

    // execute program
    env.execute("BetterCloud Flink Forward")
  }
}
