package com.github.cluelessskywatcher.halcyondb.datatests;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.cluelessskywatcher.halcyondb.HalcyonDBInstance;
import com.github.cluelessskywatcher.halcyondb.SchemaCatalog;
import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.storage.file.DatabaseFile;
import com.github.cluelessskywatcher.halcyondb.storage.file.QuickFile;
import com.github.cluelessskywatcher.halcyondb.testutils.TupleMetadataUtils;

public class SchemaCatalogTest {
    @Test
    public void testCatalogSetUp() {
        SchemaCatalog catalog = HalcyonDBInstance.getCatalog();
    }

    @Test
    public void testAddTable() {
        TupleMetadata newTupleMetadata = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.INTEGER);
        SchemaCatalog catalog = HalcyonDBInstance.getCatalog();
        DatabaseFile newFile1 = new QuickFile(1, newTupleMetadata);
        catalog.addTable(newFile1, "table1");

        Assertions.assertEquals(1, catalog.getTableId("table1"));
        Assertions.assertEquals(newTupleMetadata, catalog.getTupleMetadata("table1"));
        Assertions.assertTrue("".equals(catalog.getPrimaryKey(1)));
        Assertions.assertTrue(catalog.getTableName(1).equals("table1"));
        HalcyonDBInstance.reset();
    }

    @Test
    public void testLoadFromFile() {
        String fileName = "sampleSchema.catalog";
        SchemaCatalog catalog = HalcyonDBInstance.getCatalog();

        catalog.loadFromFile(fileName);
        Assertions.assertTrue(catalog.hasTable("students"));
        Assertions.assertTrue(catalog.hasTable("professors"));

        DataType[] profTypes = { DataType.INTEGER, DataType.INTEGER, DataType.STRING, DataType.INTEGER };
        String[] profFieldNames = { "id", "name", "subject", "yoe" };
        TupleMetadata tmProfs = new TupleMetadata(profTypes, profFieldNames);

        DataType[] studentTypes = { DataType.INTEGER, DataType.STRING, DataType.INTEGER };
        String[] studentFieldNames = { "id", "roll_no", "class_id" };
        TupleMetadata tmStudents = new TupleMetadata(studentTypes, studentFieldNames);

        Assertions.assertEquals(catalog.getTupleMetadata("professors"), tmProfs);
        Assertions.assertEquals(catalog.getTupleMetadata("students"), tmStudents);

        Assertions.assertEquals(catalog.getPrimaryKey(catalog.getTableId("students")), "id");

        Assertions.assertFalse(catalog.hasTable("school_board"));

        HalcyonDBInstance.reset();
    }
}
