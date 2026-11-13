/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.schema

import com.google.gson.stream.{JsonReader, JsonToken}
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, Type, Types}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.io.WithClose

import java.io.StringReader
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Explicit, per-file layouts for schemaless JSON attributes. Object keys are literal, case-sensitive field names;
 * a single-element array describes an array's element layout; strings describe exact VARIANT scalar kinds.
 * No layout is inferred from rows. Unmatched values are retained by parquet-java's VariantValueWriter.
 */
object VariantShredding {

  val ConfigurationKey = "geomesa.parquet.variant.shredding"
  val MaxLayoutBytes = 65536
  val MaxDepth = 16
  val MaxFields = 256

  private val Decimal = "decimal\\(([0-9]+),([0-9]+)\\)".r

  def apply(sft: SimpleFeatureType, json: String): Map[String, GroupType] = {
    require(json.length <= MaxLayoutBytes && json.getBytes(UTF_8).length <= MaxLayoutBytes,
      s"$ConfigurationKey exceeds $MaxLayoutBytes UTF-8 bytes")
    try {
      validateStrings(json)
      WithClose(new JsonReader(new StringReader(json))) { reader =>
        // Parse directly to bound recursion and reject duplicate keys.
        val parser = new LayoutParser(reader)
        val layouts = mutable.Map.empty[String, GroupType]
        reader.beginObject()
        while (reader.hasNext) {
          val attribute = reader.nextName()
          require(!layouts.contains(attribute), s"Duplicate attribute '$attribute'")
          val descriptor = sft.getDescriptor(attribute)
          require(descriptor != null, s"Unknown attribute '$attribute'")
          require(ObjectType.selectType(descriptor) == Seq(ObjectType.STRING, ObjectType.JSON) &&
            descriptor.getJsonSchema().isEmpty, s"Attribute '$attribute' must be a schemaless JSON String")
          layouts.put(attribute, parser.node(ColumnName(attribute).column, root = true, depth = 0))
        }
        reader.endObject()
        require(reader.peek() == JsonToken.END_DOCUMENT, "Trailing layout content")
        layouts.toMap
      }
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid $ConfigurationKey: ${e.getMessage}", e)
    }
  }

  // Gson 2.10's non-lenient reader accepts raw control characters in quoted strings. Reject them before
  // parsing, while preserving valid escaped controls in literal field names (for example a JSON '\n').
  private def validateStrings(json: String): Unit = {
    var quoted = false
    var escaped = false
    json.foreach { c =>
      if (quoted) {
        require(c >= ' ', "Unescaped control character in a JSON string")
        if (escaped) {
          escaped = false
        } else if (c == '\\') {
          escaped = true
        } else if (c == '"') {
          quoted = false
        }
      } else if (c == '"') {
        quoted = true
      }
    }
  }

  private class LayoutParser(reader: JsonReader) {
    private var fields = 0

    def node(name: String, root: Boolean, depth: Int, repetition: Repetition = Repetition.OPTIONAL): GroupType = {
      fields += 1
      require(fields <= MaxFields, s"Layout exceeds $MaxFields fields")
      require(depth <= MaxDepth, s"Layout exceeds depth $MaxDepth")
      val typed: Type = reader.peek() match {
        case JsonToken.STRING => scalar(reader.nextString())
        case JsonToken.BEGIN_OBJECT =>
          val builder = Types.optionalGroup()
          val names = mutable.Set.empty[String]
          reader.beginObject()
          while (reader.hasNext) {
            val field = reader.nextName()
            require(names.add(field), s"Duplicate field '$field' at ${reader.getPath}")
            builder.addField(node(field, root = false, depth + 1))
          }
          reader.endObject()
          require(names.nonEmpty, "Object layouts must select at least one field; use 'variant' for fallback only")
          builder.named("typed_value")
        case JsonToken.BEGIN_ARRAY =>
          reader.beginArray()
          require(reader.hasNext, "Array layouts require exactly one element layout")
          val element = node("element", root = false, depth + 1, Repetition.REQUIRED)
          require(!reader.hasNext, "Array layouts require exactly one element layout")
          reader.endArray()
          Types.optionalList().element(element).named("typed_value")
        case token => throw new IllegalArgumentException(s"Expected scalar, object or array layout at ${reader.getPath}: $token")
      }
      val builder = Types.buildGroup(repetition)
      if (root) {
        builder.as(variantType(1)).required(BINARY).named("metadata")
      }
      builder.optional(BINARY).named("value")
      if (typed != null) {
        builder.addField(typed)
      }
      builder.named(name)
    }

    private def scalar(kind: String): Type = {
      val builder = kind match {
        case "variant" => return null
        case "boolean" => Types.optional(BOOLEAN)
        case "string" => Types.optional(BINARY).as(stringType())
        case "byte" => Types.optional(INT32).as(intType(8, true))
        case "short" => Types.optional(INT32).as(intType(16, true))
        case "int" => Types.optional(INT32).as(intType(32, true))
        case "long" => Types.optional(INT64).as(intType(64, true))
        case "double" => Types.optional(DOUBLE)
        case Decimal(precision, scale) =>
          val p = precision.toInt
          val s = scale.toInt
          require(p >= 1 && p <= 38 && s >= 0 && s <= p, s"Invalid decimal precision/scale: $kind")
          // parquet-java matches DECIMAL4/8/16 to INT32/INT64/BINARY respectively, with identical scale.
          Types.optional(if (p <= 9) { INT32 } else if (p <= 18) { INT64 } else { BINARY }).as(decimalType(s, p))
        case _ => throw new IllegalArgumentException(s"Unsupported scalar kind '$kind' at ${reader.getPath}")
      }
      builder.named("typed_value")
    }
  }
}
