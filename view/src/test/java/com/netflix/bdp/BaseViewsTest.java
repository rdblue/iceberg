package com.netflix.bdp;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RunWith(Parameterized.class)
public class BaseViewsTest {

  @Parameterized.Parameter
  public Views views;

  @Parameterized.Parameters(name = "Run {index}: views={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { SimpleHadoopViews.of() },
        { new BaseViews(new HadoopFileIO(new Configuration()))}
    });
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void create() throws IOException {
    String viewIdentifier = folder.newFolder().getCanonicalPath();
    String sql = "SELECT * FROM db.tbl";
    Schema schema = new Schema(
        Types.NestedField.optional(1, "col1", Types.StringType.get(), "comment for col1"),
        Types.NestedField.optional(2, "col2", Types.IntegerType.get(), "comment for col2"));
    String comment = "comment for view";
    String createVersion = "Test Engine 1.0";
    String sessionCatalog = "cat";
    List<String> sessionNamespace = ImmutableList.of("default");
    String owner = "dummyOwner";

    View view = views.create(
        viewIdentifier,
        ViewMetadata.builder()
            .sql(sql)
            .schema(schema)
            .comment(comment)
            .createVersion(createVersion)
            .sessionCatalog(sessionCatalog)
            .sessionNamespace(sessionNamespace)
            .owner(owner)
            .runAsInvoker(true)
            .build());

    Assert.assertEquals(view.sql(), sql);
    Assert.assertEquals(view.schema().asStruct(), schema.asStruct());
    Assert.assertEquals(view.comment(), comment);
    Assert.assertEquals(view.createVersion(), createVersion);
    Assert.assertEquals(view.sessionCatalog(), sessionCatalog);
    Assert.assertEquals(view.sessionNamespace(), sessionNamespace);
    Assert.assertEquals(view.owner(), owner);
    Assert.assertTrue(view.runAsInvoker());

    assertViewEquals(views.load(viewIdentifier), view);
  }

  @Test
  public void createNotExists() throws IOException {
    String viewIdentifier = folder.newFolder().getCanonicalPath();
    views.create(
        viewIdentifier,
        ViewMetadata.builder().sql("dummy").build());
    Assert.assertNotNull(views.load(viewIdentifier));

    // This will fail since replace is false by default
    thrown.expect(AlreadyExistsException.class);
    views.create(
        viewIdentifier,
        ViewMetadata.builder().sql("dummy").build());
  }

  @Test
  public void drop() throws IOException {
    String viewIdentifier = folder.newFolder().getCanonicalPath();
    views.create(
        viewIdentifier,
        ViewMetadata.builder().sql("dummy").build());

    views.drop(viewIdentifier);

    thrown.expect(NotFoundException.class);
    views.load(viewIdentifier);
  }

  @Test
  public void loadNotExists() {
    String notExists = folder.getRoot() + "/" + UUID.randomUUID().toString();

    thrown.expect(NotFoundException.class);
    views.load(notExists);
  }

  @Test
  public void nestedSchema() throws IOException {
    String viewIdentifier = folder.newFolder().getCanonicalPath();
    Schema schema = new Schema(
        Types.NestedField.optional(1, "col1",
            Types.StructType.of(Types.NestedField.required(1, "foo", Types.StringType.get())),
            "comment for col1"));

    views.create(
        viewIdentifier,
        ViewMetadata.builder().sql("dummy").schema(schema).build());

    Assert.assertEquals(views.load(viewIdentifier).schema().asStruct(), schema.asStruct());
  }

  @Test
  public void defaultRunAsInvoker() throws IOException {
    String viewIdentifier = folder.newFolder().getCanonicalPath();
    views.create(
        viewIdentifier,
        ViewMetadata.builder().sql("dummy").build());

    Assert.assertEquals(
        views.load(viewIdentifier).runAsInvoker(),
        View.DEFAULT_RUNASINVOKER);
  }

  private void assertViewEquals(View v1, View v2) {
    Assert.assertNotNull(v1);
    Assert.assertNotNull(v2);
    Assert.assertEquals(v1.sql(), v2.sql());
    Assert.assertEquals(
        Optional.ofNullable(v1.schema()).map(Schema::asStruct),
        Optional.ofNullable(v2.schema()).map(Schema::asStruct));
    Assert.assertEquals(v1.comment(), v2.comment());
    Assert.assertEquals(v1.createVersion(), v2.createVersion());
    Assert.assertEquals(v1.sessionCatalog(), v2.sessionCatalog());
    Assert.assertEquals(v1.sessionNamespace(), v2.sessionNamespace());
    Assert.assertEquals(v1.owner(), v2.owner());
    Assert.assertEquals(v1.runAsInvoker(), v2.runAsInvoker());
  }
}
