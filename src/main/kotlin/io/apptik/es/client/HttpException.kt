package io.apptik.es.client

import io.vertx.reactivex.core.http.HttpClientResponse

class HttpException(val resp: HttpClientResponse) : Throwable(resp.statusMessage()) {

    fun getResponse(): HttpClientResponse {
        return resp
    }

    fun getStatusCode(): Int {
        return resp.statusCode()
    }

}



