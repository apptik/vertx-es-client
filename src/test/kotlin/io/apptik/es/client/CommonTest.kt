package io.apptik.es.client

import io.kotlintest.*
import io.kotlintest.extensions.TopLevelTest
import io.kotlintest.specs.FreeSpec
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.observers.TestObserver
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.core.Vertx
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.elasticsearch.ElasticsearchContainer
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj


//@RunWith(VertxUnitRunner::class)
class CommonTest : FreeSpec() {
    //    val ver = "6.4.3"
    val ver = "6.6.2"
    val index = "text-idx"
    lateinit var client: DefaultESClient
    lateinit var container: ElasticsearchContainer

    override fun beforeSpecClass(spec: Spec, tests: List<TopLevelTest>) {
        container = ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:$ver")
            .waitingFor(Wait.forListeningPort())
//            .waitingFor(Wait.forHttp("/").forStatusCode(204))

        container.start()
        println("Host: ${container.httpHostAddress.split(":")[0]}")
        println("Port: ${container.httpHostAddress.split(":")[1]}")

//        val restClient = RestClient.builder(HttpHost.create(container.httpHostAddress))
//            .build()
//        val response = restClient.performRequest("GET", "/")
//        println("REST resp: $response")

        client = DefaultESClient(
            container.httpHostAddress.split(":")[0],
            Integer.parseInt(container.httpHostAddress.split(":")[1]), index, Vertx.vertx()
        )
    }

    override fun afterSpecClass(spec: Spec, results: Map<TestCase, TestResult>) {
        client.close()
        container.stop()
    }

    override fun beforeTest(testCase: TestCase) {
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        client.indexExists()
            .flatMap { if (it) client.deleteIndex() else Single.just(true) }
            .test()
            .awaitCount(100)
            .assertResult(true)
    }

    init {
        "index exists" {
            client.indexExists()
                .test()
                .awaitCount(100)
                .assertResult(false)
            client.createIndex()
                .test()
                .awaitCount(100)
                .assertResult(true)
            client.indexExists()
                .test()
                .awaitCount(100)
                .assertResult(true)
        }
        "delete index" {
            client.deleteIndex()
        }
        "create index" {
            client.createIndex()
                .test()
                .awaitCount(100)
                .assertResult(true)

            client.getIndexInfo()
                .test()
                .awaitCount(100)
                .assertValue { v ->
                    val settings = v.getJsonObject(index)
                        .getJsonObject("settings").getJsonObject("index")
                    settings.getString("number_of_shards").toInt() == 5 &&
                            settings.getString("number_of_replicas").toInt() == 1
                }
        }
        "create index settings" {
            client.createIndex(json {
                obj(
                    "index" to
                            obj("number_of_shards" to 3, "number_of_replicas" to 2)
                )
            })
                .test()
                .awaitCount(100)
                .assertResult(true)

            client.getIndexInfo()
                .test()
                .awaitCount(100)
                .assertValue { v ->
                    val settings = v.getJsonObject(index)
                        .getJsonObject("settings").getJsonObject("index")
                    settings.getString("number_of_shards").toInt() == 3 &&
                            settings.getString("number_of_replicas").toInt() == 2
                }
        }
        "ensure index" {
            client.indexExists()
                .test()
                .awaitCount(100)
                .assertResult(false)
            client.ensureIndex()
                .test()
                .awaitCount(100)
                .assertComplete()
            client.indexExists()
                .test()
                .awaitCount(100)
                .assertResult(true)
        }
        "new entry" {
            client.createIndex().test().awaitCount(100).assertResult(true)
            var id: String = ""
            client.postEntry(json { obj("f1" to "data1") })
                .test()
                .awaitCount(100)
                .assertValueAt(0) { v ->
                    id = v.getString("_id")
                    v.getString("result") == "created"
                }
            client.getEntry(id)
                .test()
                .awaitCount(100)
                .assertValueAt(0) { v ->
                    v == json { obj("f1" to "data1") }
                }
        }
        "upsert entry" {
            client.createIndex().test().awaitCount(100).assertResult(true)
            val id = "123"
            client.putEntry(id, json { obj("f1" to "data1") })
                .test()
                .awaitCount(100)
                .assertValueAt(0) { v -> v.getString("result") == "created" }
            client.getEntry(id)
                .test()
                .awaitCount(100)
                .assertValueAt(0) { v ->
                    v == json { obj("f1" to "data1") }
                }
            client.putEntry(id, json { obj("f1" to "data2") })
                .test()
                .awaitCount(100)
                .assertValueAt(0) { v -> v.getString("result") == "updated" }
            client.getEntry(id)
                .test()
                .awaitCount(100)
                .assertValueAt(0) { v ->
                    v == json { obj("f1" to "data2") }
                }
        }
        "partial update entry" - {
            val id = "123"
            "given we have initial data" - {
                client.createIndex().test().awaitCount(100).assertResult(true)
                client.putEntry(id, json { obj("f1" to "data1", "f2" to "data2") })
                    .test()
                    .awaitCount(100) should haveCreatedResult()
                client.getEntry(id)
                    .test()
                    .awaitCount(100)
                    .assertValueAt(0) { v ->
                        v == json { obj("f1" to "data1", "f2" to "data2") }
                    }
                "when we partially update f2" - {
                    client.updateEntry(id, json { obj("doc" to obj("f2" to "data3")) })
                        .test()
                        .awaitCount(100) should haveUpdatedResult()
                    "then we should have old value for f1 and the new value of f2" {
                        client.getEntry(id)
                            .test()
                            .awaitCount(100) should
                                haveResult(json { obj("f1" to "data1", "f2" to "data3") })

                    }
                }
            }
        }
        "get entry" - {
            val id = "123"
            "given we have initial data" - {
                client.createIndex().test().awaitCount(100).assertResult(true)
                client.putEntry(id, json { obj("f1" to "data1") })
                    .test()
                    .awaitCount(100) should haveCreatedResult()
                "then we should be able to get the entry" {
                    client.getEntry(id)
                        .test()
                        .awaitCount(100) should
                            haveResult(json { obj("f1" to "data1") })
                }
            }
        }
        "del entry" - {
            val id = "123"
            val expected =
                JsonObject("{\"error\":{\"root_cause\":[{\"type\":\"resource_not_found_exception\",\"reason\":\"Document not found [text-idx]/[_doc]/[123]\"}],\"type\":\"resource_not_found_exception\",\"reason\":\"Document not found [text-idx]/[_doc]/[123]\"},\"status\":404}")
            "given we have initial data" - {
                client.createIndex().test().awaitCount(100).assertResult(true)
                client.putEntry(id, json { obj("f1" to "data1") })
                    .test()
                    .awaitCount(100) should haveCreatedResult()
                client.getEntry(id)
                    .test()
                    .awaitCount(100) should
                        haveResult(json { obj("f1" to "data1") })
                "when we delete the entry" - {
                    client.delEntry(id)
                        .test()
                        .awaitCount(100) should haveDeletedResult()
                    "then we should not be able to get the entry" {
                        client.getEntry(id)
                            .test()
                            .awaitCount(100) should haveResult(expected)
                    }
                }
            }
        }
        "begin scroll" {
            client.beginScroll(json { obj() }, json { obj() }, 1000)
        }
        "continue scroll" {

        }
        "search basic" {

        }
        "search filter agg params" {

        }
        "search offset limit" {

        }
        "count" {

        }
        "update by query" {

        }
        "bulk insert" - {
            val id = "123"
            "given we have empty index" - {
                client.createIndex().test().awaitCount(100).assertResult(true)
                "when we perform bulk insert" - {
                    client.bulkInsert(listOf(Pair("123", json { obj("f1" to "data1") })))
                        .test()
                        .awaitCount(100) should haveBulkResultCreated()
                    "then we should be able to get the entry" {
                        client.getEntry(id)
                            .test()
                            .awaitCount(100) should
                                haveResult(json { obj("f1" to "data1") })
                    }
                }
            }

        }
        "bulk delete" {

        }
        "put mapping" {

        }
        "ensure mapping" {

        }
        "get mapping" {
            client.getMapping()
                .test()
                .awaitCount(100)
        }
        "get mapping for field" {
            client.getMapping("f1")
                .test()
                .awaitCount(100)
        }
        "mapping exists" {

        }
    }

}

fun haveBulkResultCreated() = object : Matcher<TestObserver<JsonObject>> {
    override fun test(value: TestObserver<JsonObject>) =
        Result(
            value.values()[0]
                .getJsonArray("items").getJsonObject(0).getJsonObject("index")
                .getString("result") == "created",
            "Response '${value.values()[0]}' should have result=created",
            "Response '${value.values()[0]}' should not have result=created"
        )
}

fun haveCreatedResult() = object : Matcher<TestObserver<JsonObject>> {
    override fun test(value: TestObserver<JsonObject>) =
        Result(
            value.values()[0].getString("result") == "created",
            "Response '${value.values()[0]}' should have result=created",
            "Response '${value.values()[0]}' should not have result=created"
        )
}


fun haveUpdatedResult() = object : Matcher<TestObserver<JsonObject>> {
    override fun test(value: TestObserver<JsonObject>) =
        Result(
            value.values()[0].getString("result") == "updated",
            "Response '${value.values()[0]}' should have result=updated",
            "Response '${value.values()[0]}' should not have result=updated"
        )
}

fun haveDeletedResult() = object : Matcher<TestObserver<JsonObject>> {
    override fun test(value: TestObserver<JsonObject>) =
        Result(
            value.values()[0].getString("result") == "deleted",
            "Response '${value.values()[0]}' should have result=deleted",
            "Response '${value.values()[0]}' should not have result=deleted"
        )
}

fun haveResult(res: JsonObject) = object : Matcher<TestObserver<JsonObject>> {
    override fun test(value: TestObserver<JsonObject>) =
        Result(
            value.values()[0] == res,
            "Response '${value.values()[0]}' should be '$res'",
            "Response '${value.values()[0]}' should not be '$res'"
        )
}


fun beCreated() = object : Matcher<JsonObject> {
    override fun test(value: JsonObject) =
        Result(
            value.getString("result") == "created",
            "Response '$value' should have result=created",
            "Response '$value' should not have result=created"
        )
}

fun beUpdated() = object : Matcher<JsonObject> {
    override fun test(value: JsonObject) =
        Result(
            value.getString("result") == "updated",
            "Response '$value' should have result=updated",
            "Response '$value' should not have result=updated"
        )
}