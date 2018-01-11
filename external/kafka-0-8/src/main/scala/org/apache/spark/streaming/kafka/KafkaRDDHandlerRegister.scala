package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.Partition
import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * @author Jonathan.Gong on 2017/6/15.
  */
trait KafkaRDDHandlerRegister {
	def shortName: String

	def handle(offsetRanges: Array[OffsetRange],
	           leaders: Map[TopicAndPartition, (String, Int)],
	           kafkaParams: Map[String, String]): Array[Partition]
}

class DefaultKafkaRDDHandler extends KafkaRDDHandlerRegister {
	val shortName = "default"

	override def handle(offsetRanges: Array[OffsetRange],
	                    leaders: Map[TopicAndPartition, (String, Int)],
	                    kafkaParams: Map[String, String]): Array[Partition] = {
		offsetRanges.zipWithIndex.map { case (o, i) =>
			val (host, port) = leaders(TopicAndPartition(o.topic, o.partition))
			new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset, host, port)
		}.toArray
	}
}

class SplittableKafkaRDDHandler extends  KafkaRDDHandlerRegister with Logging {
	val shortName = "splittable"

	override def handle(offsetRanges: Array[OffsetRange],
	                    leaders: Map[TopicAndPartition, (String, Int)],
	                    kafkaParams: Map[String, String]): Array[Partition] = {
		offsetRanges.flatMap({
			range =>
				val topic = range.topic
				val p = range.partition
				val from = range.fromOffset
				val to = range.untilOffset
				val split = kafkaParams.getOrElse("xima.data.rdd.kafka.partition.split", "2").toInt
				logInfo("split is " + split)
				if (to <= from + split) {
					logInfo("the offset range is too thin, does not split. {from:" + from + ", to:" + to + "}")
					val tmpOffsetRange = OffsetRange.create(range.topic, p, from, to)
					Seq(tmpOffsetRange)
				} else {
					val ranges = new ArrayBuffer[OffsetRange]()
					val avg = (to - from) / split
					logInfo("split partition {from:" + from + ", to:" + to + "} into " + split + ". The avg is " + avg)
					var currentFrom = from
					for(i <- 1 to (split-1)) {
						val tmpTo = currentFrom + avg
						val tmpOffsetRange = OffsetRange.create(topic, p, currentFrom, tmpTo)
						logInfo(tmpOffsetRange.toString())
						ranges.append(tmpOffsetRange)
						currentFrom = tmpTo
					}
					val tmpOffsetRange = OffsetRange.create(topic, p, currentFrom, to)
					logInfo(tmpOffsetRange.toString())
					ranges.append(tmpOffsetRange)
					ranges.toSeq
				}
		}).zipWithIndex.map { case (o, i) =>
			val (host, port) = leaders(TopicAndPartition(o.topic, o.partition))
			new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset, host, port)
		}.toArray
	}
}