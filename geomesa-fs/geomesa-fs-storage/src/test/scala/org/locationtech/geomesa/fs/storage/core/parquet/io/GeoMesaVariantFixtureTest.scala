/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.google.gson.{GsonBuilder, JsonArray, JsonNull, JsonParser}
import org.apache.iceberg.mapping.{MappingUtil, NameMappingParser}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema
import org.locationtech.geomesa.fs.storage.core.parquet.schema.VariantShredding
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.ArrayBuffer

/**
 * Produces actual shared-writer files for the companion Trino interoperability tests. Set
 * geomesa.variant.fixture-directory to export a validated fixture set to an empty directory.
 * Every file has the same rows; expected.json contains JSON source strings, with JSON null for SQL NULL.
 */
class GeoMesaVariantFixtureTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  "The shared VARIANT writer" should {
    "produce validated whole, object and array fixtures for independent reader tests" >> {
      val directory = Files.createTempDirectory("geomesa-variant-fixtures")
      try {
        val sft = SimpleFeatureTypes.createType("variant-fixture", "props:String:json=true,label:String,*geom:Point:srid=4326")
        val icebergSchema = SimpleFeatureIcebergSchema.create(sft)
        val conf = Map(
          SimpleFeatureSchema.IcebergNameMappingKey -> NameMappingParser.toJson(MappingUtil.create(icebergSchema)),
          "parquet.block.row.count.limit" -> "11",
          "parquet.page.row.count.limit" -> "3",
          "parquet.page.size.row.check.min" -> "1",
          "parquet.page.size.row.check.max" -> "1")
        val files = Seq(
          "plain.parquet" -> None,
          "object.parquet" -> Some(GeoMesaVariantFixtures.ObjectLayout),
          "array.parquet" -> Some(GeoMesaVariantFixtures.ArrayLayout))

        foreach(files) { case (name, layout) =>
          val file = directory.resolve(name)
          val options = conf ++ layout.map(VariantShredding.ConfigurationKey -> _)
          WithClose(ParquetFileSystemWriter(sft, options, LocalObjectStore, file.toUri.toString)) { writer =>
            GeoMesaVariantFixtures.Rows.zipWithIndex.foreach { case (json, i) =>
              val feature = ScalaSimpleFeature.create(sft, i.toString, json, s"row-$i", "POINT (1 2)")
              feature.getUserData.put("geomesa.feature.visibility", if (i % 2 == 0) { "A" } else { "B" })
              writer.write(feature)
            }
          }
          read(file).map(normalize) mustEqual GeoMesaVariantFixtures.Rows.map(normalize)
          WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
            val metadata = reader.getFooter.getFileMetaData
            val schema = metadata.getSchema
            val variant = schema.getType(schema.getFieldIndex("props"))
            variant.getId.intValue() mustEqual icebergSchema.findField("props").fieldId()
            val groups = reader.getFooter.getBlocks.asScala
            groups.size must beGreaterThan(1)
            val columns = groups.flatMap(_.getColumns.asScala)
            val typed = columns.filter(_.getPath.toArray.last == "typed_value")
            if (layout.isEmpty) {
              typed must beEmpty
            } else {
              typed.map(c => c.getValueCount - c.getStatistics.getNumNulls).sum must beGreaterThan(0L)
              columns.filter(_.getPath.toArray.last == "value")
                .map(c => c.getValueCount - c.getStatistics.getNumNulls).sum must beGreaterThan(0L)
            }
            val rowCounts = columns.filter(_.getPath.toDotString == "props.metadata")
            rowCounts.map(_.getStatistics.getNumNulls).sum mustEqual GeoMesaVariantFixtures.Rows.count(_ == null).toLong
            val pageCounts = rowCounts.map(c => reader.readOffsetIndex(c).getPageCount)
            pageCounts.exists(_ > 1) must beTrue
          }
        }

        val expected = new JsonArray()
        GeoMesaVariantFixtures.Rows.foreach { row =>
          if (row == null) { expected.add(JsonNull.INSTANCE) } else { expected.add(row) }
        }
        Files.write(directory.resolve("expected.json"), new GsonBuilder().serializeNulls().create().toJson(expected).getBytes(UTF_8))

        sys.props.get("geomesa.variant.fixture-directory").foreach { value =>
          require(value.trim.nonEmpty, "geomesa.variant.fixture-directory must name an output directory")
          val output = Paths.get(value)
          Files.createDirectories(output)
          val names = files.map(_._1) :+ "expected.json"
          require(names.forall(name => !Files.exists(output.resolve(name))),
            "Fixture output files already exist; choose an empty directory")
          // Copy expected.json last, so its presence signals that all files were produced and validated.
          names.foreach(name => Files.copy(directory.resolve(name), output.resolve(name)))
        }
      } finally {
        WithClose(Files.walk(directory))(_.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists))
      }
      success
    }
  }

  private def normalize(json: String) = Option(json).map(JsonParser.parseString)

  private def read(file: Path): Seq[String] = {
    WithClose(ParquetFileSystemReader.builder(LocalObjectStore, file.toUri).build()) { reader =>
      val result = ArrayBuffer.empty[String]
      var feature = reader.read()
      while (feature != null) {
        result += feature.getAttribute("props").asInstanceOf[String]
        feature = reader.read()
      }
      result.toVector
    }
  }
}

private object GeoMesaVariantFixtures {

  val ObjectLayout: String =
    """{"props":{"Name":"string","name":"string","n":"byte","nested":{"flag":"boolean"},"items":[{"id":"byte"}],"a.b":"string","quote\"key":"string","amount4":"decimal(9,2)","amount8":"decimal(18,2)","amount16":"decimal(38,2)","long":"long"}}"""

  val ArrayLayout: String = """{"props":[{"id":"byte","name":"string","nested":{"flag":"boolean"}}]}"""

  private val Examples = Vector(
    """{"Name":"UPPER","name":"lower","n":1,"nested":{"flag":true},"items":[{"id":2},{"id":"later","extra":42}],"a.b":"dot","quote\"key":"quote","extra":{"keep":[1,null]},"amount4":123.45,"amount8":1234567890.12,"amount16":12345678901234567890.12,"long":2147483648}""",
    "{}", "null", null, "[]", "1", "\"scalar\"",
    """{"name":null,"n":128,"nested":null,"items":[]}""",
    """{"n":32768,"nested":{"flag":"wrong","other":true},"items":[null,{},7,{"id":null}]}""",
    """{"n":-129,"items":"wrong","name":false,"amount4":123.4,"amount8":123.45,"amount16":1234567890.12,"long":127}""",
    """[{"id":1,"name":"first","nested":{"flag":true}},{"id":2,"name":"second","unknown":[1,null]}]""",
    """[null,{},[],1,"a",{"id":"later","name":null},{"id":128,"nested":"wrong"}]""",
    """{"Name":"still upper","name":"still lower","n":-128,"nested":{},"items":[{"id":127},{"id":-128}],"a.b":"literal path"}""",
    """{"n":2147483647,"long":9223372036854775807,"amount4":-123.45,"amount8":-1234567890.12,"amount16":-12345678901234567890.12}""",
    """{"n":2147483648,"long":-9223372036854775808,"nested":{"flag":false},"items":[{"id":-129}]}""",
    """{"name":"","unknown":{"Name":"preserve case","name":"different","a.b":[{},null]}}""",
    """[[{"id":1}],[],[null],null]""",
    """[{"id":-128,"name":"last"},{"id":127},{"id":32768},{"nested":{"flag":null}}]""",
    "true", "1e100")

  val Rows: Vector[String] = Vector.fill(4)(Examples).flatten
}
