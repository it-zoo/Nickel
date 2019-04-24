package DAO

import com.arangodb.entity.BaseDocument
import consts.FieldLabels
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.*
import java.util.function.BiConsumer
import java.util.function.BinaryOperator
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collector

/**
 * ToJsonArray converter from baseDocument
 */
class ToJsonArray : Collector<BaseDocument, JsonArray, JsonArray> {
    override fun characteristics(): MutableSet<Collector.Characteristics> {
        return EnumSet.of(Collector.Characteristics.IDENTITY_FINISH)
    }

    override fun supplier(): Supplier<JsonArray> {
        return Supplier { JsonArray() }
    }

    override fun accumulator(): BiConsumer<JsonArray, BaseDocument> {
        return BiConsumer { t, u ->
            t.add(baseDocumentToJsonObject(u))
        }
    }

    private fun baseDocumentToJsonObject(document: BaseDocument): JsonObject {
        val json = JsonObject()
        json.put(FieldLabels.Key.name, document.key)
        json.put(FieldLabels.Id.name, document.id)

        for (entry in document.properties) {
            json.put(entry.key, entry.value)
        }
        return json
    }

    override fun combiner(): BinaryOperator<JsonArray> {
        return BinaryOperator { t, u ->
            t.addAll(u)
        }
    }

    override fun finisher(): Function<JsonArray, JsonArray> {
        return Function.identity()
    }
}