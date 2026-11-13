/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.google.gson.JsonParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg.{PartitionSpec, TableProperties}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.{ColumnIOFactory, LocalInputFile}
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.variant.{Variant, VariantBuilder, VariantJsonParser}
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, Partition}
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.fs.storage.core.iceberg.{IcebergCatalog, SimpleFeatureIcebergSchema}
import org.locationtech.geomesa.fs.storage.core.parquet.schema.{SimpleFeatureParquetSchema, VariantShredding}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Path}
import scala.collection.mutable.ArrayBuffer

class VariantShreddingTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  sequential

  private val sft = SimpleFeatureTypes.createType("shredding", "props:String:json=true,label:String,*geom:Point:srid=4326")
  private val key = VariantShredding.ConfigurationKey

  private def withFile(layout: String, rows: Seq[String])(test: Path => Unit): Unit = {
    val file = Files.createTempFile("geomesa-shredding", ".parquet")
    try {
      val conf = Option(layout).map(key -> _).toMap ++ Map(
        "parquet.block.row.count.limit" -> "7", "parquet.page.row.count.limit" -> "3",
        "parquet.page.size.row.check.min" -> "1", "parquet.page.size.row.check.max" -> "1")
      WithClose(ParquetFileSystemWriter(sft, conf, LocalObjectStore, file.toUri.toString)) { writer =>
        rows.zipWithIndex.foreach { case (json, i) =>
          writer.write(ScalaSimpleFeature.create(sft, i.toString, json, s"label-$i", "POINT (1 2)"))
        }
      }
      test(file)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  private def read(file: Path, transform: Option[SimpleFeatureType] = None): Seq[String] = {
    val conf = new PlainParquetConfiguration()
    transform.foreach(SimpleFeatureParquetSchema.setSft(conf, _))
    WithClose(ParquetFileSystemReader.builder(LocalObjectStore, file.toUri).withConf(conf).build()) { reader =>
      val rows = ArrayBuffer.empty[String]
      var feature = reader.read()
      while (feature != null) {
        rows += feature.getAttribute("props").asInstanceOf[String]
        feature = reader.read()
      }
      rows.toVector
    }
  }

  private def physicalRows(file: Path): Seq[Group] = {
    WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
      val schema = reader.getFooter.getFileMetaData.getSchema
      val io = new ColumnIOFactory().getColumnIO(schema)
      val rows = ArrayBuffer.empty[Group]
      var pages = reader.readNextRowGroup()
      while (pages != null) {
        val records = io.getRecordReader(pages, new GroupRecordConverter(schema))
        (0L until pages.getRowCount).foreach(_ => rows += records.read())
        pages = reader.readNextRowGroup()
      }
      rows.toVector
    }
  }

  private def normalize(json: String) = Option(json).map(JsonParser.parseString)

  // The Iceberg scan exposes its native Variant value for schemaless attributes. Serialize both parts to
  // compare against the original JSON independently of Iceberg's object equality or diagnostic toString.
  private def icebergJson(value: AnyRef): String = {
    if (value == null) { null } else {
      val variant = value.asInstanceOf[org.apache.iceberg.variants.Variant]
      val metadata = ByteBuffer.allocate(variant.metadata().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN)
      val bytes = ByteBuffer.allocate(variant.value().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN)
      variant.metadata().writeTo(metadata, 0)
      variant.value().writeTo(bytes, 0)
      VariantJsonWriter.toJson(new Variant(bytes, metadata))
    }
  }

  "Explicit VARIANT layouts" should {
    "preserve the unshredded default" >> {
      val schema = SimpleFeatureParquetSchema(sft, Map.empty).messageType
      val plain = schema.getType(schema.getFieldIndex("props")).asGroupType()
      plain.containsField("typed_value") must beFalse
      SimpleFeatureParquetSchema(sft, Map(key -> "{}")).messageType mustEqual
        SimpleFeatureParquetSchema(sft, Map.empty).messageType
    }

    "validate the attribute and layout before opening a file" >> {
      val invalid = Seq(
        """{"missing":"string"}""", """{"label":"string"}""", """{"props":"uuid"}""",
        """{"props":{}}""", """{"props":[]}""", """{"props":["string","byte"]}""",
        """{"props":{"x":"byte","x":"string"}}""", """{"props":"byte","props":"string"}""",
        """{"props":"decimal(39,2)"}""", """{"props":"decimal(3,4)"}""",
        """{"props":null}""", """{"props":"string"} {}""", "{props:'string'}",
        "{\"props\":{\"unescaped\nnewline\":\"string\"}}", "{\"props\":{\"unescaped\ttab\":\"string\"}}")
      foreach(invalid) { layout =>
        SimpleFeatureParquetSchema(sft, Map(key -> layout)) must throwA[IllegalArgumentException]
      }
    }

    "reject layouts for structural JSON" >> {
      val structural = SimpleFeatureTypes.createType("structural", "props:String:json=true")
      structural.getDescriptor("props").getUserData.put(AttributeOptions.OptJsonSchema, "\"string\"")
      VariantShredding(structural, """{"props":"string"}""") must throwA[IllegalArgumentException]
    }

    "reject an invalid layout before creating the output file" >> {
      val directory = Files.createTempDirectory("geomesa-invalid-layout")
      val file = directory.resolve("invalid.parquet")
      try {
        ParquetFileSystemWriter(sft, Map(key -> """{"missing":"string"}"""), LocalObjectStore,
          file.toUri.toString) must throwA[IllegalArgumentException]
        Files.exists(file) must beFalse
      } finally {
        Files.deleteIfExists(file)
        Files.deleteIfExists(directory)
      }
    }

    "bound layout bytes, recursion and field count" >> {
      val deep = "{\"props\":" + ("[" * (VariantShredding.MaxDepth + 1)) + "\"string\"" +
        ("]" * (VariantShredding.MaxDepth + 1)) + "}"
      val wide = (0 until VariantShredding.MaxFields).map(i => s""""f$i":"string"""").mkString("{\"props\":{", ",", "}}")
      val multibyte = "{\"props\":{\"" + ("\u03a9" * 40000) + "\":\"string\"}}"
      foreach(Seq(deep, wide, multibyte, " " * (VariantShredding.MaxLayoutBytes + 1))) { layout =>
        VariantShredding(sft, layout) must throwA[IllegalArgumentException]
      }
    }

    "accept bounded layouts and preserve escaped control characters in literal keys" >> {
      val deep = "{\"props\":" + ("[" * VariantShredding.MaxDepth) + "\"string\"" +
        ("]" * VariantShredding.MaxDepth) + "}"
      val wide = (0 until VariantShredding.MaxFields - 1)
        .map(i => s""""f$i":"string"""").mkString("{\"props\":{", ",", "}}")
      foreach(Seq(deep, wide))(layout => VariantShredding(sft, layout).keySet mustEqual Set("props"))
      val escaped = """{"props":{"line\nbreak":"string","tab\tkey":"string","quote\"key":"string"}}"""
      withFile(escaped, Seq("""{"line\nbreak":"line","tab\tkey":"tab","quote\"key":"quote"}""")) { file =>
        val value = physicalRows(file).head.getGroup("props", 0).getGroup("typed_value", 0)
        value.getGroup("line\nbreak", 0).getBinary("typed_value", 0).toStringUsingUTF8 mustEqual "line"
        value.getGroup("tab\tkey", 0).getBinary("typed_value", 0).toStringUsingUTF8 mustEqual "tab"
        value.getGroup("quote\"key", 0).getBinary("typed_value", 0).toStringUsingUTF8 mustEqual "quote"
      }
      success
    }

    "populate typed leaves while preserving nulls, unknown fields, arrays and late type changes" >> {
      val layout = """{"props":{"Name":"string","name":"string","n":"byte","nested":{"flag":"boolean"},"items":[{"id":"byte"}],"a.b":"string","quote\"key":"string"}}"""
      val examples = Seq(
        """{"Name":"UPPER","name":"lower","n":1,"nested":{"flag":true},"items":[{"id":2},{"id":"later","extra":42}],"a.b":"dot","quote\"key":"quote","extra":{"keep":[1,null]}}""",
        "{}", "null", null, "[]", "1", "\"scalar\"",
        """{"name":null,"n":128,"nested":null,"items":[]}""",
        """{"n":32768,"nested":{"flag":"wrong","other":true},"items":[null,{},7,{"id":null}]}""",
        """{"n":-129,"items":"wrong","name":false}"""
      )
      val rows = Vector.fill(3)(examples).flatten
      withFile(layout, rows) { file =>
        read(file).map(normalize) mustEqual rows.map(normalize)
        val transform = SimpleFeatureTypes.createType("transformed", "props:String:json=true")
        read(file, Some(transform)).map(normalize) mustEqual rows.map(normalize)
        val physical = physicalRows(file)
        val value = physical.head.getGroup("props", 0)
        value.getType.getLogicalTypeAnnotation mustEqual LogicalTypeAnnotation.variantType(1)
        value.getFieldRepetitionCount("value") mustEqual 1 // unknown root key
        val typed = value.getGroup("typed_value", 0)
        typed.getGroup("Name", 0).getBinary("typed_value", 0).toStringUsingUTF8 mustEqual "UPPER"
        typed.getGroup("name", 0).getBinary("typed_value", 0).toStringUsingUTF8 mustEqual "lower"
        typed.getGroup("n", 0).getInteger("typed_value", 0) mustEqual 1
        typed.getGroup("n", 0).getFieldRepetitionCount("value") mustEqual 0
        physical(3).getFieldRepetitionCount("props") mustEqual 0 // SQL NULL
        physical(2).getGroup("props", 0).getFieldRepetitionCount("value") mustEqual 1 // VARIANT null
        val changed = physical(7).getGroup("props", 0).getGroup("typed_value", 0).getGroup("n", 0)
        changed.getFieldRepetitionCount("typed_value") mustEqual 0
        changed.getFieldRepetitionCount("value") mustEqual 1 // SHORT does not silently widen BYTE
        WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
          val blocks = reader.getFooter.getBlocks.asScala
          blocks.size must beGreaterThan(1)
          val metadata = blocks.flatMap(_.getColumns.asScala).filter(_.getPath.toDotString == "props.metadata")
          metadata.map(_.getStatistics.getNumNulls).sum mustEqual 3L
        }
      }
      success
    }

    "bound fallback bytes to one value while retaining dictionary IDs and reserved user keys" >> {
      val layout = """{"props":{"a":"byte","items":[{"n":"byte","value":"string","typed_value":"string","metadata":"string"}],"obj":"byte","value":"string","typed_value":"string","metadata":"string","tail":"string"}}"""
      val json = """{"a":null,"items":[{"n":128,"value":"user value","typed_value":"user typed","metadata":"user metadata"},{"n":1.2300,"extra":{"ar":[null,{"late":true}]}},null,[{"deep":{"key":1.23400}}]],"obj":{"first":null,"alpha":{"z":[12,null]},"later":1.2300},"value":"root value","typed_value":"root typed","metadata":"root metadata","tail":"""" +
        ("x" * 10000) + """","unknown":{"nested":[1,{"notSelected":false}]}}"""
      val rows = Seq(json, json.replace("x" * 10000, "y" * 32), json)
      withFile(layout, rows) { file =>
        val actual = read(file)
        assert(actual.map(normalize) == rows.map(normalize), "All rows must retain their full JSON values")
        val parsed = VariantJsonParser.parseJson(actual.head)
        parsed.getFieldByKey("items").getElementAtIndex(1).getFieldByKey("n").getType mustEqual Variant.Type.DECIMAL4
        parsed.getFieldByKey("items").getElementAtIndex(1).getFieldByKey("n").getDecimal.scale() mustEqual 4
        parsed.getFieldByKey("obj").getFieldByKey("later").getDecimal.scale() mustEqual 4
        val root = physicalRows(file).head.getGroup("props", 0)
        val typed = root.getGroup("typed_value", 0)
        assert(typed.getGroup("a", 0).getBinary("value", 0).length() == 1, "VARIANT null must occupy one byte")
        foreach(Seq("value", "typed_value", "metadata")) { name =>
          typed.getGroup(name, 0).getBinary("typed_value", 0).toStringUsingUTF8 mustEqual ("root " +
            (if (name == "typed_value") { "typed" } else { name }))
        }
        // Walk the physical schema rather than user JSON names: only primitive `value` leaves are fallbacks.
        var fallbacks = 0
        def check(group: Group): Unit = {
          group.getType.getFields.asScala.zipWithIndex.foreach { case (field, index) =>
            (0 until group.getFieldRepetitionCount(index)).foreach { occurrence =>
              if (!field.isPrimitive) {
                check(group.getGroup(index, occurrence))
              } else if (field.getName == "value") {
                val binary = group.getBinary(index, occurrence)
                val exact = new VariantBuilder()
                exact.appendEncodedValue(binary.toByteBuffer)
                assert(binary.length() == exact.encodedValue().remaining(),
                  s"Fallback in ${group.getType.getName} includes bytes beyond its encoded value")
                fallbacks += 1
              }
            }
          }
        }
        check(root)
        assert(fallbacks >= 7, s"Expected scalar, array, object and residual fallbacks, found $fallbacks")
      }
      success
    }

    "match integer widths and decimal representations exactly" >> {
      val cases = Seq(
        ("byte", "127", "128", Variant.Type.BYTE),
        ("short", "128", "1", Variant.Type.SHORT),
        ("int", "32768", "2147483648", Variant.Type.INT),
        ("long", "2147483648", "32768", Variant.Type.LONG),
        ("decimal(9,2)", "123.45", "123.4", Variant.Type.DECIMAL4),
        ("decimal(18,2)", "1234567890.12", "123.45", Variant.Type.DECIMAL8),
        ("decimal(38,2)", "12345678901234567890.12", "1234567890.12", Variant.Type.DECIMAL16),
        ("double", "1e100", "1", Variant.Type.DOUBLE))
      foreach(cases) { case (kind, matching, fallback, expectedType) =>
        VariantJsonParser.parseJson(matching).getType mustEqual expectedType
        withFile(s"""{"props":"$kind"}""", Seq(matching, fallback, "null", null)) { file =>
          val actual = read(file)
          actual.map(normalize) mustEqual Seq(matching, fallback, "null", null).map(normalize)
          VariantJsonParser.parseJson(actual.head).getType mustEqual expectedType
          if (kind.startsWith("decimal")) {
            VariantJsonParser.parseJson(actual.head).getDecimal.scale() mustEqual 2
          }
          val rows = physicalRows(file)
          rows.head.getGroup("props", 0).getFieldRepetitionCount("typed_value") mustEqual 1
          rows.head.getGroup("props", 0).getFieldRepetitionCount("value") mustEqual 0
          rows(1).getGroup("props", 0).getFieldRepetitionCount("typed_value") mustEqual 0
          rows(1).getGroup("props", 0).getFieldRepetitionCount("value") mustEqual 1
        }
        success
      }
    }

    "support root arrays and fallback-only elements" >> {
      foreach(Seq("""{"props":["string"]}""", """{"props":["variant"]}""")) { layout =>
        val rows = Seq("""["a",null,{},[],1,"b"]""", "[]", "null", null, "{}")
        withFile(layout, rows) { file =>
          read(file).map(normalize) mustEqual rows.map(normalize)
          val array = physicalRows(file).head.getGroup("props", 0).getGroup("typed_value", 0)
          array.getType.getType("list").asGroupType().getType("element").getRepetition mustEqual Repetition.REQUIRED
        }
        success
      }
    }

    "use a second feature type without depending on attribute names or mutable configuration" >> {
      val other = SimpleFeatureTypes.createType("other", "Payload:String:json=true,details:String:json=true,ordinary:String")
      val conf = new PlainParquetConfiguration()
      SimpleFeatureParquetSchema.setSft(conf, other)
      conf.set(key, """{"Payload":{"Case Sensitive":"string"},"details":["byte"]}""")
      val first = SimpleFeatureParquetSchema(conf).get.messageType
      conf.set(key, """{"details":"long"}""")
      val second = SimpleFeatureParquetSchema(conf).get.messageType
      first.getType(first.getFieldIndex("payload")).asGroupType().containsField("typed_value") must beTrue
      second.getType(second.getFieldIndex("payload")).asGroupType().containsField("typed_value") must beFalse
      first must not(beEqualTo(second))
    }

    "register and query mixed layouts through Iceberg with correct VARIANT null counts" >> {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("variant-writer-test"))
      val directory = Files.createTempDirectory("geomesa-shredding-iceberg")
      val properties = IcebergCatalog.sftTableProperties(sft) ++ Map(TableProperties.FORMAT_VERSION -> "3")
      val table = new HadoopTables(new Configuration()).create(SimpleFeatureIcebergSchema.create(sft),
        PartitionSpec.unpartitioned(), properties.asJava, directory.resolve("table").toUri.toString)
      val storage = FileSystemStorage(table, Seq.empty, SimpleFeatureIcebergSchema(table), Map.empty)
      try {
        val rows = Seq("""{"name":"typed","extra":123}""", "{}", "null", null, "[]")
        val layouts = Seq(Map.empty[String, String], Map(key -> """{"props":{"name":"string"}}"""),
          Map(key -> """{"props":["byte"]}"""))
        layouts.zipWithIndex.foreach { case (conf, batch) =>
          WithClose(storage.copy(conf = conf).getWriter(Partition.None)) { writer =>
            rows.zipWithIndex.foreach { case (json, i) =>
              writer.write(ScalaSimpleFeature.create(sft, s"$batch-$i", json, "label", "POINT (1 2)"))
            }
          }
        }
        WithClose(table.newScan().includeColumnStats().planFiles()) { tasks =>
          val files = tasks.iterator().asScala.map(_.file()).toVector
          files must haveSize(3)
          foreach(files) { file =>
            file.recordCount() mustEqual rows.size.toLong
            file.nullValueCounts().get(table.schema().findField("props").fieldId()) mustEqual java.lang.Long.valueOf(1L)
          }
        }
        WithClose(storage.getReader(new Query(sft.getTypeName), 1)) { reader =>
          val actual = reader.map(f => f.getID -> normalize(icebergJson(f.getAttribute("props")))).toMap
          val expected = layouts.indices.flatMap(b => rows.zipWithIndex.map { case (row, i) => s"$b-$i" -> normalize(row) }).toMap
          actual mustEqual expected
        }
      } finally {
        storage.close()
        WithClose(Files.walk(directory))(_.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists))
      }
      success
    }
  }
}
