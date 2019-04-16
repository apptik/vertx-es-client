@file:Suppress("NOTHING_TO_INLINE")

package io.apptik.es.client

import io.vertx.core.json.JsonObject
import java.util.*
import kotlin.collections.ArrayList

const val TYPE_OBJECT = "object"

val types = listOf(
        "text", "keyword", "float", "half_float", "scaled_float", "double", "byte", "short", "ip",
        "integer", "long", "date", "boolean", "binary", "object", "nested", "geo_point", "geo_shape")

val dateFormats =  listOf("date", "date_hour", "date_hour_minute", "date_hour_minute_second",
        "date_hour_minute_second_fraction", "date_hour_minute_second_millis", "date_optional_time",
        "date_time", "date_time_no_millis", "hour", "hour_minute", "hour_minute_second",
        "hour_minute_second_fraction", "hour_minute_second_millis", "ordinal_date", "ordinal_date_time",
        "ordinal_date_time_no_millis", "time", "time_no_millis", "t_time", "t_time_no_millis",
        "week_date", "week_date_time", "weekDateTimeNoMillis", "week_year", "weekyearWeek",
        "weekyearWeekDay", "year", "year_month", "year_month_day", "epoch_millis", "epoch_second")

/**
 * Returns a list of properties where type() and fields() and properties() can be called
 */
inline fun JsonObject.properties(): Optional<JsonObject> {
    return Optional.of(this.getJsonObject("properties"))
}

/**
 *
 */
inline fun JsonObject.type(): String {
    var res = this.getString("type")
    if(res==null && this.properties().isPresent) {
        res = TYPE_OBJECT
    }
    return res
}

inline fun JsonObject.fields(): JsonObject {
    return this.getJsonObject("fields")
}


