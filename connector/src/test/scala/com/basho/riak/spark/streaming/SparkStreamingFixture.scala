package com.basho.riak.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.{After, Before}
import org.slf4j.{Logger, LoggerFactory}

trait SparkStreamingFixture {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  protected var sc: SparkContext

  protected var ssc: StreamingContext = _

  protected val batchDuration = Seconds(1)

  @Before
  def startStreamingContext(): Unit = {
    ssc = new StreamingContext(sc, batchDuration)
    logger.info("Streaming context created")
  }

  @After
  def stopStreamingContext(): Unit = {
    Option(ssc).foreach(_.stop())
    logger.info("Streaming context stopped")
  }
}
