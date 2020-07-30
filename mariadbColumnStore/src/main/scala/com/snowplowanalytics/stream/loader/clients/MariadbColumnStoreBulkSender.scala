/**
 * Copyright (c) 2020 Software.com
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.stream.loader
package clients

// AWS
import com.amazonaws.auth.AWSCredentialsProvider

// Java
import com.mariadb.columnstore.api._
import org.slf4j.LoggerFactory

// Scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import cats.Id
import cats.effect.{IO, Timer}
import cats.data.Validated
import cats.syntax.validated._

import retry.implicits._
import retry.{RetryDetails, RetryPolicy}
import retry.CatsEffect._

import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.stream.loader.Config.StreamLoaderConfig

/**
 * Main ES component responsible for inserting data into a specific index,
 * data is passed here by [[Emitter]] */
class MariadbColumnStoreBulkSender(
  database: String,
  table: String,
  // mapping_file: String,
  columnstore_xml: Option[String],
  // delimiter: String,
  // date_format: String,
  // enclose_by_character: String,
  // escape_character: String,
  // read_cache_size: Int,
  // header: Boolean,
  // ignore_malformed_csv: Boolean,
  // err_log: String,
  val maxConnectionWaitTimeMs: Long,
  region: String,
  awsSigning: Boolean,
  credentialsProvider: AWSCredentialsProvider,
  val tracker: Option[Tracker[Id]],
  val maxAttempts: Int = 6
) extends BulkSender[EmitterJsonInput] {
  require(maxAttempts > 0)
  require(maxConnectionWaitTimeMs > 0)

  import MariadbColumnStoreBulkSender._

  override val log = LoggerFactory.getLogger(getClass)

  private val client = columnstore_xml match {
    case Some(xml) => new ColumnStoreDriver(xml)
    // Use COLUMNSTORE_INSTALL_DIR environment variable and then the default
    // default path of /etc/columnstore/Columnstore.xml to find a Columnstore.xml
    case _         => new ColumnStoreDriver
  }

  override def close(): Unit =
    log.info("Closing MariaDB ColumnStore BulkSender")

  override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    log.info("MariaDB ColumnStore send")
    log.info("records: {}", records);
    val connectionAttemptStartTime = System.currentTimeMillis()
    implicit def onErrorHandler: (Throwable, RetryDetails) => IO[Unit] =
      BulkSender.onError(log, tracker, connectionAttemptStartTime)
    implicit def retryPolicy: RetryPolicy[IO] =
      BulkSender.delayPolicy[IO](maxAttempts, maxConnectionWaitTimeMs)

    // oldFailures - failed at the transformation step
    val (successes, oldFailures) = records.partition(_._2.isValid)
    val jsonRecords = successes.collect {
      case (_, Validated.Valid(jsonRecord)) => jsonRecord
    }

    val newFailures: List[EmitterJsonInput] = BulkSender
      .futureToTask(Future(writeRecords(jsonRecords)))
      .retryingOnSomeErrors(BulkSender.exPredicate)
      .attempt
      .unsafeRunSync() match {
        case Right(s) => s
        case Left(f) =>
          log.error(
            s"Shutting down application as unable to connect to MariaDB ColumnStore for over $maxConnectionWaitTimeMs ms",
            f)
          // if the request failed more than it should have we force shutdown
          forceShutdown()
          Nil
      }

    log.info(s"Emitted ${jsonRecords.size - newFailures.size} records to MariaDB ColumnStore")
    if (newFailures.nonEmpty) logHealth()

    val allFailures = oldFailures ++ newFailures

    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")

    allFailures
  }

  def writeRecords(jsonRecords: List[JsonRecord]): List[EmitterJsonInput] = {
    log.info("Creating BulkInsert")
    val bulkInsert: ColumnStoreBulkInsert = client.createBulkInsert(database, table, 0: Short, 0)
    val failures = ListBuffer[EmitterJsonInput]()

    jsonRecords.asJava.forEach { jsonRecord =>
      try {
        log.info("jsonRecord json: {}", jsonRecord.json)
        utils.extractEventId(jsonRecord.json) match {
          case Some(id) =>
            bulkInsert.setColumn(0, id)
          case None =>
            bulkInsert.setColumn(0, "")
        }
        bulkInsert.setColumn(1, jsonRecord.json.noSpaces)
        bulkInsert.writeRow()
      } catch {
        case e: ColumnStoreException => {
          log.error("Failure: {}" + e)
          failures += e.getMessage -> jsonRecord.valid
        }
      }
    }
    bulkInsert.commit()

    val summary: ColumnStoreSummary = bulkInsert.getSummary()
    log.info("Execution time: {}" + summary.getExecutionTime());
    log.info("Rows inserted: {}" + summary.getRowsInsertedCount());
    log.info("Truncation count: {}" + summary.getTruncationCount());
    log.info("Saturated count: {}" + summary.getSaturatedCount());
    log.info("Invalid count: {}" + summary.getInvalidCount());
    log.info("newFailures: {}", failures)

    failures.toList
  }

  /** Logs the cluster health */
  override def logHealth(): Unit = log.info("MariaDB ColumnStore health is green")
}

object MariadbColumnStoreBulkSender {
  implicit val ioTimer: Timer[IO] =
    IO.timer(concurrent.ExecutionContext.global)

  def apply(
    config: StreamLoaderConfig,
    tracker: Option[Tracker[Id]]
  ): MariadbColumnStoreBulkSender = {
    new MariadbColumnStoreBulkSender(
      config.mariadb_columnstore.client.database,
      config.mariadb_columnstore.client.table,
      // config.mariadb_columnstore.client.mapping_file,
      config.mariadb_columnstore.client.columnstore_xml,
      // config.mariadb_columnstore.client.delimiter,
      // config.mariadb_columnstore.client.date_format,
      // config.mariadb_columnstore.client.enclose_by_character,
      // config.mariadb_columnstore.client.escape_character,
      // config.mariadb_columnstore.client.read_cache_size,
      // config.mariadb_columnstore.client.header,
      // config.mariadb_columnstore.client.ignore_malformed_csv,
      // config.mariadb_columnstore.client.err_log,
      config.mariadb_columnstore.client.maxTimeout,
      config.mariadb_columnstore.aws.region,
      config.mariadb_columnstore.aws.signing,
      CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey),
      tracker,
      config.mariadb_columnstore.client.maxRetries
    )
  }
}
