package com.oldtan.config

import org.apache.commons.logging.LogFactory
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}

trait LazyLog {
  val log = LogFactory.getLog(this.getClass)
}

trait LazyEsClient {
  val esClient = new RestHighLevelClient(RestClient.builder(new HttpHost("huaweioldtan", 9200, "http")))
}
