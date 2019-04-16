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

package io.apptik.es.client


import io.reactivex.Single
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.reactivex.Completable


interface EsClient {

    /**
     * Check if Elasticsearch is running and if it answers to a simple request
     * @return `true` if Elasticsearch is running, `false`
     * otherwise
     */
    val isRunning: Single<Boolean>

    /**
     * Close the client and release all resources
     */
    fun close()


    fun getEntry(id: String): Single<JsonObject>

    /**
     * Crete new with ES generated ID
     */
    fun postEntry(data: JsonObject): Single<JsonObject>

    /**
     * complete replace or create new with specific ID
     */
    fun putEntry(id: String, data: JsonObject): Single<JsonObject>


    /**
     * Partial update
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
     */
    fun updateEntry(id: String, data: JsonObject): Single<JsonObject>


    fun delEntry(id: String): Single<JsonObject>

    /**
     * Insert a number of documents in one bulk request
     * @param documents a list of document IDs and actual documents to insert
     * @return the parsed bulk response from the server
     * @see .bulkResponseHasErrors
     * @see .bulkResponseGetErrorMessage
     */
    fun bulkInsert(documents: List<Pair<String, JsonObject>>): Single<JsonObject>

    /**
     * Perform a search and start scrolling over the result documents
     * @param query the query to send
     * @param parameters the elasticsearch parameters
     * @param timeout the time after which the returned scroll id becomes invalid
     * @return an object containing the search hits and a scroll id that can
     * be passed to [.continueScroll] to get more results
     */
    fun beginScroll(query: JsonObject,
                    parameters: JsonObject, timeout: Int): Single<JsonObject> {
        return beginScroll(query, null, parameters, timeout)
    }

    /**
     * Perform a search and start scrolling over the result documents. You can
     * either specify a `query`, a `postFilter` or both,
     * but one of them is required.
     * @param query the query to send (may be `null`, in this case
     * `postFilter` must be set)
     * @param postFilter a filter to apply (may be `null`, in this case
     * `query` must be set)
     * @param parameters the elasticsearch parameters
     * @param timeout the time after which the returned scroll id becomes invalid
     * @return an object containing the search hits and a scroll id that can
     * be passed to [.continueScroll] to get more results
     */
    fun beginScroll(query: JsonObject,
                    postFilter: JsonObject?, parameters: JsonObject, timeout: Int): Single<JsonObject> {
        return beginScroll(query, postFilter, null, parameters, timeout)
    }

    /**
     * Perform a search, apply an aggregation, and start scrolling over the
     * result documents. You can either specify a `query`, a
     * `postFilter` or both, but one of them is required.
     * @param query the query to send (may be `null`, in this case
     * `postFilter` must be set)
     * @param postFilter a filter to apply (may be `null`, in this case
     * `query` must be set)
     * @param aggregations the aggregations to apply. Can be `null`
     * @param parameters the elasticsearch parameters
     * @param timeout the time after which the returned scroll id becomes invalid
     * @return an object containing the search hits and a scroll id that can
     * be passed to [.continueScroll] to get more results
     */
    fun beginScroll(query: JsonObject,
                    postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject, timeout: Int): Single<JsonObject>

    /**
     * Continue scrolling through search results. Call
     * [.beginScroll] to get a scroll id
     * @param scrollId the scroll id
     * @param timeout the time after which the scroll id becomes invalid
     * @return an object containing new search hits and possibly a new scroll id
     */
    fun continueScroll(scrollId: String, timeout: Int): Single<JsonObject>

    /**
     * Perform a search. The result set might not contain all documents. If you
     * want to scroll over all results use
     * [.beginScroll]
     * instead.
     * @param query the query to send
     * @param parameters the elasticsearch parameters
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun search(query: JsonObject, parameters: JsonObject): Single<JsonObject> {
        return search(query, null, null, parameters)
    }

    /**
     * Perform a search. The result set might not contain all documents. If you
     * want to scroll over all results use
     * [.beginScroll]
     * instead.
     * @param query the query to send (may be `null`)
     * @param postFilter a filter to apply (may be `null`)
     * @param parameters the elasticsearch parameters
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun search(query: JsonObject,
               postFilter: JsonObject, parameters: JsonObject): Single<JsonObject> {
        return search(query, postFilter, null, parameters)
    }

    /**
     * Perform a search. The result set might not contain all documents. If you
     * want to scroll over all results use
     * [.beginScroll]
     * instead.
     * @param query the query to send (may be `null`)
     * @param postFilter a filter to apply (may be `null`)
     * @param aggregations the aggregations to apply (may be `null`)
     * @param parameters the elasticsearch parameters
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun search(query: JsonObject,
               postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject): Single<JsonObject>

    fun search(query: JsonObject, offset: Int, limit: Int,
                        postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject): Single<JsonObject>

        /**
     * Perform a count operation. The result is the number of documents
     * matching the query (without the documents themselves). If no query
     * is given, the total number of documents is returned.
     * @param query the query to send (may be `null`)
     * @return the number of documents matching the query
     */
    fun count(query: JsonObject): Single<Long>

    /**
     * Perform an update operation. The update script is applied to all
     * documents that match the post filter.
     * @param postFilter a filter to apply (may be `null`)
     * @param script the update script to apply
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun updateByQuery(postFilter: JsonObject,
                      script: JsonObject): Single<JsonObject>

    /**
     * Delete a number of documents in one bulk request
     * @param ids the IDs of the documents to delete
     * @return the parsed bulk response from the server
     * @see .bulkResponseHasErrors
     * @see .bulkResponseGetErrorMessage
     */
    fun bulkDelete(ids: JsonArray): Single<JsonObject>

    /**
     * Check if the index exists
     * @return a single emitting `true` if the index exists or
     * `false` otherwise
     */
    fun indexExists(): Single<Boolean>

    /**
     * Check if the mapping of the index exists
     * @return a single emitting `true` if the mapping of
     * the index exists or `false` otherwise
     */
    fun mappingExists() : Single<Boolean>

    /**
     * Create the index
     * @return a single emitting `true` if the index creation
     * was acknowledged by Elasticsearch, `false` otherwise
     */
    fun createIndex(): Single<Boolean>

    /**
     * Create the index with settings.
     * @param settings the settings to set for the index.
     * @return a single emitting `true` if the index creation
     * was acknowledged by Elasticsearch, `false` otherwise
     */
    fun createIndex(settings: JsonObject): Single<Boolean>

    /**
     * Convenience method that makes sure the index exists. It first calls
     * [.indexExists] and then [.createIndex] if the index does
     * not exist yet.
     * @return a single that will emit a single item when the index has
     * been created or if it already exists
     */
    fun ensureIndex(): Completable

    /**
     * Convenience method that makes sure the index is fresh i.e. it is deleted if already exists.It first calls
     * [.deleteIndex] and then [.createIndex] if the index does
     * exist already.
     * @return a single that will emit a single item when the index has
     * been created or if it already exists
     */
    fun resetIndex(settings : JsonObject): Completable


    /**
     * Add mapping
     * @param mapping the mapping to set
     * @return a single emitting `true` if the operation
     * was acknowledged by Elasticsearch, `false` otherwise
     */
    fun putMapping(mapping: JsonObject): Single<Boolean>

    /**
     * Convenience method that makes sure the given mapping exists. It first calls
     * [.mappingExists] and then [.putMapping]
     * if the mapping does not exist yet.
     * @param mapping the mapping to set
     * @return a single that will emit a single item when the mapping has
     * been created or if it already exists
     */
    fun ensureMapping(mapping: JsonObject): Completable

    /**
     * Get mapping
     * @return the parsed mapping response from the server
     */
    fun getMapping(): Single<JsonObject>

    /**
     * Get mapping
     * @param field the field
     * @return the parsed mapping response from the server
     */
    fun getMapping(field: String?): Single<JsonObject>

    /**
     * Check if the given bulk response contains errors
     * @param response the bulk response
     * @return `true` if the response has errors, `false`
     * otherwise
     * @see .bulkInsert
     * @see .bulkDelete
     * @see .bulkResponseGetErrorMessage
     */
    fun bulkResponseHasErrors(response: JsonObject): Boolean {
        return response.getBoolean("errors", false)!!
    }

    /**
     * Builds an error message from a bulk response containing errors
     * @param response the response containing errors
     * @return the error message or null if the bulk response does not contain
     * errors
     * @see .bulkInsert
     * @see .bulkDelete
     * @see .bulkResponseHasErrors
     */
    fun bulkResponseGetErrorMessage(response: JsonObject): String? {
        if (!bulkResponseHasErrors(response)) {
            return null
        }

        val res = StringBuilder()
        res.append("Errors in bulk operation:")
        val items = response.getJsonArray("items", JsonArray())
        for (o in items) {
            val jo = o as JsonObject
            for (key in jo.fieldNames()) {
                val op = jo.getJsonObject(key)
                if (bulkResponseItemHasErrors(op)) {
                    res.append(bulkResponseItemGetErrorMessage(op))
                }
            }
        }
        return res.toString()
    }

    /**
     * Check if an item of a bulk response has an error
     * @param item the item
     * @return `true` if the item has an error, `false`
     * otherwise
     */
    fun bulkResponseItemHasErrors(item: JsonObject): Boolean {
        return item.getJsonObject("error") != null
    }

    /**
     * Builds an error message from a bulk response item
     * @param item the item
     * @return the error message or null if the bulk response item does not
     * contain an error
     */
    fun bulkResponseItemGetErrorMessage(item: JsonObject): String? {
        if (!bulkResponseItemHasErrors(item)) {
            return null
        }

        val res = StringBuilder()
        val error = item.getJsonObject("error")
        if (error != null) {
            val id = item.getString("_id")
            val type = error.getString("type")
            val reason = error.getString("reason")
            res.append("\n[id: [")
            res.append(id)
            res.append("], type: [")
            res.append(type)
            res.append("], reason: [")
            res.append(reason)
            res.append("]]")
        }

        return res.toString()
    }

    fun deleteIndex(): Single<Boolean>
    fun getIndexInfo(): Single<JsonObject>
}