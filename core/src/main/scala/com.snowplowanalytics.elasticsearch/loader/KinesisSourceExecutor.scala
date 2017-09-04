/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
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

package com.snowplowanalytics
package elasticsearch.loader

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}

// Tracker
import snowplow.scalatracker.Tracker

// This project
import clients.ElasticsearchSender
import sinks._
import model._

/**
 * Boilerplate class for Kinesis Conenector
 *
 * @param streamType the type of stream, good, bad or plain-json
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 * @param config the KCL configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param elasticsearchSender function for sending to elasticsearch 
 * @param tracker a Tracker instance
 */
class KinesisSourceExecutor(
  streamType: StreamType,
  documentIndex: String,
  documentType: String,
  config: KinesisConnectorConfiguration,
  goodSink: Option[ISink],
  badSink: ISink,
  elasticsearchSender: ElasticsearchSender,
  tracker: Option[Tracker] = None
) extends KinesisConnectorExecutorBase[ValidatedRecord, EmitterInput] {

  initialize(config)
  override def getKinesisConnectorRecordProcessorFactory = {
    new KinesisConnectorRecordProcessorFactory[ValidatedRecord, EmitterInput](
      new KinesisElasticsearchPipeline(streamType, documentIndex, documentType, goodSink, badSink, elasticsearchSender, tracker), config)
  }
}