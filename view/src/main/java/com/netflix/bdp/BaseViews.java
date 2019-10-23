package com.netflix.bdp;

import com.fasterxml.jackson.core.JsonGenerator;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The Views implementation Based on FileIO.
 */
@Slf4j
@Data
public class BaseViews implements Views {

  static final String METADATA_FOLDER_NAME = "metadata";

  private final FileIO io;

  private static OutputStream createFile(OutputFile outputFile, boolean replace) {
    if (replace) {
      return outputFile.createOrOverwrite();
    } else {
      return outputFile.create();
    }
  }

  @Override
  public View create(
      String viewIdentifier,
      View definition,
      boolean replace) {
    String filePath = filePath(viewIdentifier);
    LOG.info("Create view file {}", filePath);

    ViewMetadata view = ViewMetadata.builder()
        .sql(definition.sql())
        .schema(definition.schema())
        .comment(definition.comment())
        .createVersion(definition.createVersion())
        .sessionCatalog(definition.sessionCatalog())
        .sessionNamespace(definition.sessionNamespace())
        .owner(definition.owner())
        .runAsInvoker(definition.runAsInvoker())
        .build();

    try (OutputStream os = createFile(io.newOutputFile(filePath), replace);
         JsonGenerator generator = JsonUtil.factory().createGenerator(os)) {
      generator.useDefaultPrettyPrinter();
      ViewMetadataParser.toJson(view, generator);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to create file %s", filePath);
    }

    return view;
  }

  @Override
  public View load(String viewIdentifier) {
    String filePath = filePath(viewIdentifier);
    LOG.info("Read view file {}", filePath);
    try (InputStream is = io.newInputFile(filePath).newStream()) {
      return ViewMetadataParser.fromJson(JsonUtil.mapper().readTree(is));
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to read file %s", filePath);
    }
  }

  @Override
  public void drop(String viewIdentifier) {
    String filePath = filePath(viewIdentifier);
    LOG.info("Delete view file {}", filePath);
    io.deleteFile(filePath);
  }

  private String filePath(String viewIdentifier) {
    return String.join("/",
        viewIdentifier,
        METADATA_FOLDER_NAME,
        "view.json");
  }
}
