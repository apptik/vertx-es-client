/*
 * Copyright (c) 2018. AppTik Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
original idea from
https://github.com/georocket/georocket/blob/master/georocket-server/src/main/java/io/georocket/index/elasticsearch/RemoteElasticsearchClient.java
 */

package io.apptik.es.client


import io.netty.handler.codec.json.JsonObjectDecoder
import io.vertx.core.Future
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpMethod
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.reactivex.RxHelper
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.core.buffer.Buffer
import io.vertx.reactivex.core.http.HttpClient
import io.vertx.reactivex.core.http.HttpClientRequest
import io.reactivex.Completable
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.functions.Consumer
import io.vertx.reactivex.core.http.HttpClientResponse
import java.lang.System.err
import io.vertx.core.AsyncResult


/**
 * An Elasticsearch client for Vert.X using the HTTP API
 *
 * Connect to an Elasticsearch instance
 * @param host the host to connect to
 * @param port the port on which Elasticsearch is listening for HTTP
 * requests (most likely 9200)
 * @param index the index to query against
 * @param vertx a Vert.x instance
 */
class DefaultESClient(
    host: String, port: Int,
    /**
     * The index to query against
     */
    private val index: String, vertx: Vertx
) : EsClient {


    /**
     * The HTTP client used to talk to Elasticsearch
     */
    private val client: HttpClient

    override val isRunning: Single<Boolean>
        get() {
            val req = client.head("/")
            return performRequest(req, null).map { v -> true }.onErrorReturn { t -> false }
        }

    init {

        val clientOptions = HttpClientOptions()
            .setDefaultHost(host)
            .setDefaultPort(port)
            .setKeepAlive(true)
        client = vertx.createHttpClient(clientOptions)
    }

    override fun close() {
        client.close()
    }

    override fun getEntry(id: String): Single<JsonObject> {
        val uri = "/$index/_doc/$id/_source"
        val source = JsonObject()
        return performRequest(HttpMethod.GET, uri, null)
    }

    override fun delEntry(id: String): Single<JsonObject> {
        val uri = "/$index/_doc/$id"
        return performRequest(HttpMethod.DELETE, uri, null)
    }

    override fun postEntry(data: JsonObject): Single<JsonObject> {
        val uri = "/$index/_doc/"
        return performRequest(HttpMethod.POST, uri, data.toString())
    }

    override fun putEntry(id: String, data: JsonObject): Single<JsonObject> {
        val uri = "/$index/_doc/$id"
        return performRequest(HttpMethod.PUT, uri, data.toString())
    }

    //partial update
    /**
     * Partially update document note this method accepts raw Elasticsearch 'script' Object
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html">
     *     https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html</a>
     * @data partial document update script
     */
    override fun updateEntry(id: String, data: JsonObject): Single<JsonObject> {
        val uri = "/$index/_doc/$id/_update"
        val update = JsonObject().put("doc", data)
        return performRequest(HttpMethod.POST, uri, data.toString())
    }

    override fun bulkInsert(documents: List<Pair<String, JsonObject>>): Single<JsonObject> {
        val uri = "/$index/_doc/_bulk"

        // prepare the whole body now because it's much faster to send
        // it at once instead of using HTTP chunked mode.
        val body = StringBuilder()
        for (e in documents) {
            val id = e.first
            val source = e.second.encode()
            val subject = JsonObject().put("_id", id)
            body.append("{\"index\":" + subject.encode() + "}\n" + source + "\n")
        }

        return performRequest(HttpMethod.POST, uri, body.toString())
    }

    override fun beginScroll(
        query: JsonObject, postFilter: JsonObject?, aggregations: JsonObject?,
        parameters: JsonObject, timeout: Int
    ): Single<JsonObject> {
        var uri = "/$index/_doc/_search"
        uri += "?scroll=$timeout"

        val source = JsonObject()
        parameters.forEach { entry -> source.put(entry.key, entry.value) }

        source.put("query", query)

        if (postFilter != null) {
            source.put("post_filter", postFilter)
        }
        if (aggregations != null) {
            source.put("aggs", aggregations)
        }

        // sort by doc (fastest way to scroll)
        source.put("sort", JsonArray().add("_doc"))

        return performRequest(HttpMethod.GET, uri, source.encode())
    }

    override fun continueScroll(scrollId: String, timeout: Int): Single<JsonObject> {
        val uri = "/_search/scroll"

        val source = JsonObject()
        source.put("scroll", timeout)
        source.put("scroll_id", scrollId)

        return performRequest(HttpMethod.GET, uri, source.encode())
    }

    override fun search(
        query: JsonObject,
        postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject
    ): Single<JsonObject> {
        return search(
            query, 0, defSearchLimit, postFilter, aggregations, parameters
        )
    }

    override fun search(
        query: JsonObject, offset: Int, limit: Int,
        postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject
    ): Single<JsonObject> {
        val uri = "/$index/_doc/_search"

        val source = JsonObject()
        parameters.forEach { entry -> source.put(entry.key, entry.value) }

        source.put("from", offset)
        source.put("size", limit)

        source.put("query", query)

        if (postFilter != null) {
            source.put("post_filter", postFilter)
        }
        if (aggregations != null) {
            source.put("aggs", aggregations)
        }

        return performRequest(HttpMethod.GET, uri, source.encode())
    }

    override fun count(query: JsonObject): Single<Long> {
        val uri = "/$index/_doc/_count"

        val source = JsonObject().put("query", query)

        return performRequest(HttpMethod.GET, uri, source.encode())
            .flatMap { sr ->
                val l = sr.getLong("count")
                if (l == null) {
                    Single.error<Long>(
                        NoStackTraceThrowable(
                            "Could not count documents"
                        )
                    )
                }
                Single.just(l)
            }
    }

    override fun updateByQuery(
        postFilter: JsonObject,
        script: JsonObject
    ): Single<JsonObject> {
        val uri = "/$index/_doc/_update_by_query"

        val source = JsonObject()
        source.put("post_filter", postFilter)
        source.put("script", script)


        return performRequest(HttpMethod.POST, uri, source.encode())
    }

    override fun bulkDelete(ids: JsonArray): Single<JsonObject> {
        val uri = "/$index/_doc/_bulk"

        // prepare the whole body now because it's much faster to send
        // it at once instead of using HTTP chunked mode.
        val body = StringBuilder()
        for (i in 0 until ids.size()) {
            val id = ids.getString(i)
            val subject = JsonObject().put("_id", id)
            body.append("{\"delete\":" + subject.encode() + "}\n")
        }

        return performRequest(HttpMethod.POST, uri, body.toString())
    }

    override fun indexExists(): Single<Boolean> {
        return exists("/$index")
    }

    override fun getIndexInfo(): Single<JsonObject> {
        return performRequest(HttpMethod.GET, "/$index", null)
    }


    /**
     * Check if the given URI exists by sending an empty request
     * @param uri uri to check
     * @return an observable emitting `true` if the request
     * was successful or `false` otherwise
     */
    private fun exists(uri: String): Single<Boolean> {
        return performRequest(HttpMethod.HEAD, uri, null)
            .map { true }
            .onErrorResumeNext { t ->
                if (t is HttpException && t.getStatusCode() == 404) {
                    Single.just(false)
                } else {
                    Single.error(t)
                }
            }
    }

    override fun deleteIndex(): Single<Boolean> {
        val uri = "/$index"

        return performRequest(HttpMethod.DELETE, uri, null)
            .map { res -> res.getBoolean("acknowledged", false) }
    }

    override fun createIndex(): Single<Boolean> {
        return createIndex(JsonObject())
    }

    override fun createIndex(settings: JsonObject): Single<Boolean> {
        val uri = "/$index"

        val body = JsonObject().put("settings", settings).encode()

        return performRequest(HttpMethod.PUT, uri, body)
            .map { res -> res.getBoolean("acknowledged", false) }
    }

    /**
     * Ensure the Elasticsearch index exists
     * @return an observable that will emit a single item when the index has
     * been created or if it already exists
     */
    override fun ensureIndex(): Completable {
        // check if the index exists
        return indexExists().flatMapCompletable { exists ->
            if (exists) {
                Completable.complete()
            } else {
                // index does not exist. create it.
                createIndex().flatMapCompletable { ack ->
                    if (ack) {
                        Completable.complete()
                    } else {
                        Completable.error(
                            NoStackTraceThrowable("Index creation was not acknowledged by Elasticsearch"))
                    }
                }
            }
        }
    }

    override fun resetIndex(settings : JsonObject): Completable {
        return deleteIndex().ignoreElement().andThen(createIndex(settings).ignoreElement())
    }


    override fun putMapping(mapping: JsonObject): Single<Boolean> {
        val uri = "/$index/_mapping/_doc"
        return performRequest(HttpMethod.PUT, uri, mapping.encode())
            .map { res -> res.getBoolean("acknowledged", false) }
    }

    /**
     * Ensure the Elasticsearch mapping exists
     * @return an observable that will emit a single item when the mapping has
     * been created or if it already exists
     */
    override fun ensureMapping(mapping: JsonObject): Completable {
        return mappingExists().flatMapCompletable { exists ->
            if (exists) {
                Completable.complete()
            } else {
                //  create the mapping.
                putMapping(mapping).flatMapCompletable { ack ->
                    if (ack) {
                        Completable.complete()
                    }
                    Completable.error(NoStackTraceThrowable("Mapping creation " + "was not acknowledged by Elasticsearch"))
                }
            }
        }
    }

    override fun getMapping(): Single<JsonObject> {
        return getMapping(null)
    }


    override fun mappingExists(): Single<Boolean> {
        return exists("/$index/_mapping/_doc")
    }

    override fun getMapping(field: String?): Single<JsonObject> {
        var uri = "/$index/_mapping/_doc"
        if (field != null) {
            uri += "/field/$field"
        }
        return performRequest(HttpMethod.GET, uri, "")
    }

    private fun performRequest(method: HttpMethod, uri: String, body: String?): Single<JsonObject> {
        return performRequest(client.request(method, uri), body)
    }

    /**
     * Perform an HTTP request
     * @param req the request to perform
     * @param body the body to send in the request (may be null)
     * @return an observable emitting the parsed response body (may be null if no
     * body was received)
     */
    private fun performRequest(
        req: HttpClientRequest,
        body: String?
    ): Single<JsonObject> {

        req.exceptionHandler { e -> e.printStackTrace() }

        var res: Single<JsonObject> = req.toObservable()
            .doOnNext { resp ->
                log.info("ES Response ${resp.statusCode()} :: ${resp.statusMessage()}")
            }
            .map { resp ->
                if ((resp.request().method() == HttpMethod.HEAD) && resp.statusCode() > 399) {
                    throw HttpException(resp)
                }
                resp
            }
            .flatMap(HttpClientResponse::toObservable)
            .collectInto(Buffer.buffer()) { b, i -> b.appendBuffer(i) }
            .map { buff ->
                if (buff.length() < 1) {
                    JsonObject()
                } else {
                    try {
                        buff.toJsonObject()
                    } catch (e: Throwable) {
                        JsonObject().put("resp", buff.toString())
                    }
                }
            }
            .doOnError { err ->
                log.error("ES Got Response error", err.localizedMessage)
//                err.printStackTrace()
            }
            .doOnSuccess {
                log.info("ES Got Response body: $it")
            }

        if (body != null) {
            req.isChunked = false
            val buf = Buffer.buffer(body)
            req.putHeader("Accept", "application/json")
            req.putHeader("Content-Type", "application/json")
            req.putHeader("Content-Length", buf.length().toString())
            res = res.doOnSubscribe { _ ->
                log.info("Execute ES call with body: " + req.absoluteURI())
                req.end(buf)
            }
        } else {
            res = res.doOnSubscribe { _ ->
                log.info("Execute ES call: " + req.absoluteURI())
                req.end()
            }
        }
        log.info("Prepare ES call: " + req.absoluteURI())

        return res
    }

    companion object {
        private val log = org.slf4j.LoggerFactory.getLogger(DefaultESClient::class.java)
    }
}


