/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg.{MetricsConfig, PartitionSpec, TableProperties}
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.types.Conversions
import org.apache.iceberg.hadoop.HadoopTables
import org.geotools.api.data.Query
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.iceberg.{IcebergCatalog, SimpleFeatureIcebergSchema}
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.core.parquet.io.{ParquetFileMetrics, ParquetFileSystemWriter, VariantJsonWriter}
import org.locationtech.geomesa.fs.storage.core.parquet.schema.VariantShredding
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files

class FileRegistrationTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  "File registration" should {
    "publish large VARIANT values with unknown statistics while retaining other column metrics" in {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("geomesa-registration-test"))
      foreach(Seq(Map.empty[String, String],
        Map(VariantShredding.ConfigurationKey -> """{"props":{"n":"byte"}}"""),
        Map(VariantShredding.ConfigurationKey -> """{"props":{"n":"byte"}}""",
          "parquet.column.statistics.enabled#props.metadata" -> "false"))) { conf =>
        val directory = Files.createTempDirectory("geomesa-large-variant-registration")
        val sft = SimpleFeatureTypes.createType("large", "name:String,props:String:json=true,small:String:json=true,*geom:Point")
        val properties = IcebergCatalog.sftTableProperties(sft) ++ Map(TableProperties.FORMAT_VERSION -> "3")
        val table = new HadoopTables(new Configuration()).create(SimpleFeatureIcebergSchema.create(sft),
          PartitionSpec.unpartitioned(), properties.asJava, directory.resolve("table").toUri.toString)
        val storage = FileSystemStorage(table, Seq.empty, SimpleFeatureIcebergSchema(table), conf)
        try {
          val input = directory.resolve("input.parquet").toUri
          val json = """{"n":1,"blob":"""" + ("x" * (if (conf.contains("parquet.column.statistics.enabled#props.metadata")) { 8 } else { 20000 })) + """"}"""
          WithClose(ParquetFileSystemWriter(storage.schema, conf, table.io(), input.toString, NoOpObserver)) { writer =>
            writer.write(ScalaSimpleFeature.create(sft, "a", "alpha", json, "1", "POINT (1 2)"))
            writer.write(ScalaSimpleFeature.create(sft, "b", "omega", json, null, "POINT (3 4)"))
          }
          val file = table.io().newInputFile(input.toString)
          if (conf.isEmpty) {
            ParquetUtil.fileMetrics(file, MetricsConfig.forTable(table)) must throwA[java.util.NoSuchElementException]
          }
          val registered = storage.metadata.register(Map(Partition.None -> Seq(input)))
          val data = registered.head
          val props = table.schema().findField("props").fieldId()
          val name = table.schema().findField("name").fieldId()
          val small = table.schema().findField("small").fieldId()
          assert(data.recordCount() == 2L)
          assert(!data.valueCounts().containsKey(props) && !data.nullValueCounts().containsKey(props))
          assert(!data.lowerBounds().containsKey(props) && !data.upperBounds().containsKey(props))
          assert(data.valueCounts().get(name) == 2L && data.nullValueCounts().get(name) == 0L)
          assert(Conversions.fromByteBuffer(table.schema().findType(name), data.lowerBounds().get(name)).toString == "alpha")
          assert(Conversions.fromByteBuffer(table.schema().findType(name), data.upperBounds().get(name)).toString == "omega")
          assert(data.valueCounts().get(small) == 2L && data.nullValueCounts().get(small) == 1L)
          val countsOnly = MetricsConfig.fromProperties(Map(
            (TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "name") -> "counts",
            (TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "small") -> "none").asJava)
          val limited = ParquetFileMetrics.fileMetrics(file, countsOnly)
          assert(limited.valueCounts().get(name) == 2L && !limited.lowerBounds().containsKey(name))
          assert(!limited.valueCounts().containsKey(small))
          val truncated = ParquetFileMetrics.fileMetrics(file, MetricsConfig.fromProperties(Map(
            (TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "name") -> "truncate(2)").asJava))
          assert(Conversions.fromByteBuffer(table.schema().findType(name), truncated.lowerBounds().get(name)).toString == "al")
          table.refresh()
          assert(table.currentSnapshot().addedDataFiles(table.io()).asScala.size == 1)
          WithClose(storage.getReader(new Query(sft.getTypeName), 1)) { reader =>
            val values = reader.map { feature =>
              // Iceberg scans expose the native Variant, not its JSON text or diagnostic toString.
              val value = feature.getAttribute("props").asInstanceOf[org.apache.iceberg.variants.Variant]
              val metadata = ByteBuffer.allocate(value.metadata().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN)
              val bytes = ByteBuffer.allocate(value.value().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN)
              value.metadata().writeTo(metadata, 0)
              value.value().writeTo(bytes, 0)
              feature.getID -> com.google.gson.JsonParser.parseString(
                VariantJsonWriter.toJson(new org.apache.parquet.variant.Variant(bytes, metadata)))
            }.toMap
            assert(values == Map("a" -> com.google.gson.JsonParser.parseString(json),
              "b" -> com.google.gson.JsonParser.parseString(json)))
          }
          val corrupt = directory.resolve("corrupt.parquet")
          Files.write(corrupt, Array[Byte](1, 2, 3, 4))
          ParquetFileMetrics.fileMetrics(table.io().newInputFile(corrupt.toUri.toString),
            MetricsConfig.forTable(table)) must throwA[Exception]
        } finally {
          storage.close()
          WithClose(Files.walk(directory))(_.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists))
        }
        success
      }
    }

    "record attempt properties atomically with its files and retain them across unrelated appends" in {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("geomesa-registration-test"))
      val directory = Files.createTempDirectory("geomesa-registration")
      val sft = SimpleFeatureTypes.createType("registration", "name:String,*geom:Point:srid=4326")
      val properties = IcebergCatalog.sftTableProperties(sft) ++ Map(TableProperties.FORMAT_VERSION -> "3")
      val table = new HadoopTables(new Configuration()).create(SimpleFeatureIcebergSchema.create(sft),
        PartitionSpec.unpartitioned(), properties.asJava, directory.resolve("table").toUri.toString)
      val storage = FileSystemStorage(table, Seq.empty, SimpleFeatureIcebergSchema(table), Map.empty)
      try {
        val input = directory.resolve("input.parquet").toUri
        WithClose(ParquetFileSystemWriter(storage.schema, Map.empty, table.io(), input.toString, NoOpObserver)) { writer =>
          writer.write(ScalaSimpleFeature.create(sft, "exported", "exported", "POINT (1 2)"))
        }
        val attempt = "registration-attempt"
        val registered = storage.metadata.register(Map(Partition.None -> Seq(input)),
          Map("geomesa.export.attempt-id" -> attempt))
        val committed = table.currentSnapshot()
        committed.summary().get("geomesa.export.attempt-id") mustEqual attempt
        registered must haveLength(1)
        committed.addedDataFiles(table.io()).asScala.map(_.location()).toSeq mustEqual registered.map(_.location())
        registered.head.location() must not(beEqualTo(input.toString))
        registered.head.recordCount() mustEqual 1L

        WithClose(storage.getWriter(Partition.None)) { writer =>
          writer.write(ScalaSimpleFeature.create(sft, "other", "other", "POINT (3 4)"))
        }
        table.refresh()
        table.currentSnapshot().snapshotId() must not(beEqualTo(committed.snapshotId()))
        val matches = table.snapshots().asScala.filter(_.summary().get("geomesa.export.attempt-id") == attempt).toSeq
        matches.map(_.snapshotId()) mustEqual Seq(committed.snapshotId())
        matches.head.addedDataFiles(table.io()).asScala.map(_.location()).toSeq mustEqual registered.map(_.location())

        val latest = table.currentSnapshot().snapshotId()
        storage.metadata.register(Map.empty[Partition, Seq[java.net.URI]],
          Map("geomesa.export.attempt-id" -> "empty-attempt")) must beEmpty
        table.refresh()
        table.currentSnapshot().snapshotId() mustEqual latest
      } finally {
        storage.close()
        WithClose(Files.walk(directory))(_.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists))
      }
    }
  }
}
