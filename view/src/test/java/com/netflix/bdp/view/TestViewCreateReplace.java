package com.netflix.bdp.view;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestViewCreateReplace extends TestViewBase {

    @Test
    public void testViewCreateReplace() {
        ViewVersionMetadata viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
        ViewDefinition oldViewMetadata = viewVersionMetadata.definition();
        ViewDefinition viewMetadata = ViewDefinition.of("select sum(1) from base_tab", oldViewMetadata.schema(),
                "", new ArrayList<>());
        TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

        // Change the view sql
        viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
        oldViewMetadata = viewVersionMetadata.definition();
        Assert.assertEquals(oldViewMetadata.sql(), "select sum(1) from base_tab");

        // Change the schema, session catalog and engine version
        Schema newSchema = new Schema(
                required(1, "id", Types.IntegerType.get()),
                required(2, "intData", Types.IntegerType.get())
        );
        viewMetadata = ViewDefinition.of("select id, intData from base_tab", newSchema,
                 "new catalog", new ArrayList<>());
        TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

        // Assert that the replaced view has the correct changes
        viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
        oldViewMetadata = viewVersionMetadata.definition();
        Assert.assertEquals(oldViewMetadata.schema(), newSchema);
        Assert.assertEquals(oldViewMetadata.sessionCatalog(), "new catalog");
        Assert.assertEquals(viewVersionMetadata.currentVersion().summary().properties().get(CommonViewConstants.ENGINE_VERSION),
                "TestEngine");

        // Expect to see three versions
        Assert.assertEquals(viewVersionMetadata.currentVersionId(), 3);
        Assert.assertEquals(viewVersionMetadata.versions().size(), 3);
        Assert.assertEquals(viewVersionMetadata.versions().get(2).parentId().longValue(), 2);
    }
}
