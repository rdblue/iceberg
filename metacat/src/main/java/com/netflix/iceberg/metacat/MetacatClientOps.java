package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;

class MetacatClientOps extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MetacatClientOps.class);
  private static final String SPARK_PROVIDER = "spark.sql.sources.provider";
  private static final Predicate<Exception> RETRY_IF = exc ->
      !exc.getClass().getCanonicalName().contains("Unrecoverable");

  private Configuration conf;
  private final Client client;
  private final String catalog;
  private final String database;
  private final String table;
  private HadoopFileIO fileIO;

  MetacatClientOps(Configuration conf, Client client, String catalog, String database, String table) {
    this.conf = conf;
    this.client = client;
    this.catalog = catalog;
    this.database = database;
    this.table = table;
    refresh();
  }

  @Override
  public synchronized TableMetadata refresh() {
    String metadataLocation = null;
    try {
      TableDto tableInfo = client.getApi().getTable(catalog, database, table,
          true /* send table fields, partition keys */,
          false /* do not send user definition metadata (foreign key info) */,
          false /* do not send user data metadata (?) */);

      Map<String, String> tableProperties = tableInfo.getMetadata();
      String tableType = tableProperties.get(TABLE_TYPE_PROP);

      Preconditions.checkArgument(
          tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
          "Invalid table, not Iceberg: %s.%s.%s", catalog, database, table);

      metadataLocation = tableProperties.get(METADATA_LOCATION_PROP);
      Preconditions.checkNotNull(metadataLocation,
          "Invalid table, missing metadata_location: %s.%s.%s", catalog, database, table);

    } catch (MetacatNotFoundException e) {
      // if metadata has been loaded for this table and is now gone, throw an exception
      // otherwise, assume the table doesn't exist yet.
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(format(
            "No such Metacat table: %s.%s.%s", catalog, database, table));
      }
    }

    refreshFromMetadataLocation(metadataLocation, RETRY_IF, 20);

    return current();
  }

  @Override
  public synchronized void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    ObjectNode watermarks = flinkWatermarkProperties(base, metadata);

    boolean threw = true;
    try {
      StorageDto serde = new StorageDto();
      // set the Spark data source provider
      serde.setInputFormat(FileInputFormat.class.getName());
      serde.setOutputFormat(FileOutputFormat.class.getName());
      serde.setSerializationLib(LazySimpleSerDe.class.getName());
      serde.setUri(metadata.location());

      TableDto newTableInfo = new TableDto();
      newTableInfo.setName(QualifiedName.ofTable(catalog, database, table));
      newTableInfo.setSerde(serde);
      newTableInfo.setDataExternal(true);

      // forward any changed flink.watermark properties to definition metadata
      if (watermarks != null) {
        newTableInfo.setDefinitionMetadata(watermarks);
      }

      if (base != null) {
        newTableInfo.setMetadata(ImmutableMap.of(
            SPARK_PROVIDER, ICEBERG_TABLE_TYPE_VALUE,
            TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
            METADATA_LOCATION_PROP, newMetadataLocation,
            PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation()
        ));

        client.getApi().updateTable(catalog, database, table, newTableInfo);

      } else {
        // if creating a migrated table, copy the TTL settings and other definition metadata
        boolean isMigrated = Boolean.parseBoolean(
            metadata.properties().getOrDefault("migrated-from-hive", "false"));
        if (isMigrated) {
          Preconditions.checkArgument(table.endsWith("_iceberg"),
              "Expected temporary table name ending in '_iceberg': %s", table);
          String backupTableName = table.substring(0, table.length() - 8) + "_hive";

          try {
            TableDto table = client.getApi().getTable(catalog, database, backupTableName,
                true /* send table fields, partition keys */,
                true /* send user definition metadata (including ttl settings) */,
                false /* do not send user data metadata (?) */);

            // copy all of the definition metadata
            newTableInfo.setDefinitionMetadata(table.getDefinitionMetadata());

          } catch (MetacatNotFoundException e) {
            LOG.warn("Cannot find backup table %s.%s.%s, not copying definition metadata",
                catalog, database, backupTableName);
          }
        }

        newTableInfo.setMetadata(ImmutableMap.of(
            SPARK_PROVIDER, ICEBERG_TABLE_TYPE_VALUE,
            TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
            METADATA_LOCATION_PROP, newMetadataLocation
        ));

        client.getApi().createTable(catalog, database, table, newTableInfo);
      }

      threw = false;

    } catch (MetacatPreconditionFailedException e) {
      throw new CommitFailedException(e, "Failed to commit due to conflict");

    } catch (MetacatBadRequestException | MetacatUserMetadataException e) {
      throw new ValidationException(e,
          "Failed to commit: invalid request", e.getMessage());

    } catch (MetacatException e) {
      throw new RuntimeIOException(new IOException(e), "Failed to commit");

    } finally {
      if (threw) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(newMetadataLocation);
      }
    }

    requestRefresh();
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

  public static List<FieldDto> fieldDtos(Schema schema) {
    List<FieldDto> fields = Lists.newArrayList();

    for (int i = 0; i < schema.columns().size(); i++) {
      final Types.NestedField field = schema.columns().get(i);
      FieldDto fieldInfo = new FieldDto();
      fieldInfo.setPos(i);
      fieldInfo.setName(field.name());
      fieldInfo.setType(getMetacatTypeName(field.type()));
      fieldInfo.setIsNullable(field.isOptional());

      fields.add(fieldInfo);
    }
    return fields;
  }

  public static String getMetacatTypeName(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
        throw new UnsupportedOperationException("Metacat does not support time fields");
      case TIMESTAMP:
        return "timestamp";
      case STRING:
      case UUID:
        return "string";
      case FIXED:
        return "binary";
      case BINARY:
        return "binary";
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return format("decimal(%s,%s)", decimalType.precision(), decimalType.scale()); //TODO may be just decimal?
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream().map(
                f -> format("%s:%s", f.name(), getMetacatTypeName(f.type()))
        ).collect(Collectors.joining(","));
        return format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return format("array<%s>", getMetacatTypeName(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return format("map<%s,%s>", getMetacatTypeName(mapType.keyType()), getMetacatTypeName(mapType.valueType()));
      default:
        throw new UnsupportedOperationException(type +" is not supported");
    }
  }

  private static final String FLINK_WATERMARK_PREFIX = "flink.watermark.";

  private static ObjectNode flinkWatermarkProperties(TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null,
        current.properties(),
        FLINK_WATERMARK_PREFIX);

    if (updates.isEmpty()) {
      return null;
    }

    ObjectNode watermarks = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, String> update : updates.entrySet()) {
      watermarks.put(update.getKey().replace(FLINK_WATERMARK_PREFIX, ""), Long.parseLong(update.getValue()));
    }

    ObjectNode metadata = JsonNodeFactory.instance.objectNode();
    metadata.put("flink.watermarks", watermarks);

    return metadata;
  }

  private static Map<String, String> changedProperties(Map<String, String> base, Map<String, String> current,
                                                       String prefix) {
    Map<String, String> result = Maps.newHashMap();
    Set<Map.Entry<String, String>> baseSet = base != null ? base.entrySet() : Collections.emptySet();

    for (Map.Entry<String, String> entry : current.entrySet()) {
      // forward the properties that are not in the base set (changed) and match the prefix
      if (!baseSet.contains(entry) && entry.getKey().startsWith(prefix)) {
        result.put(entry.getKey(), entry.getValue());
      }
    }

    return result;
  }
}
