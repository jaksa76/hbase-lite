package me.jaksa.hbase.lite;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TableTest {
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATableWithNoColumns() throws Exception {
        Table.extractColumns("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATableWithNoFamily() throws Exception {
        Table.extractColumns("fam1:col1,col2,fam1:col3");
    }

    @Test()
    public void testCreatingATableWithSpacesInColumnsString() throws Exception {
        List<Table.Column> columns = Table.extractColumns("fam1 :col1 , fam1: col2, fam2 : col1");

        assertThat(columns.size(), is(3));

        assertThat(columns.get(0).family, is("fam1"));
        assertThat(columns.get(0).name, is("col1"));

        assertThat(columns.get(1).family, is("fam1"));
        assertThat(columns.get(1).name, is("col2"));

        assertThat(columns.get(2).family, is("fam2"));
        assertThat(columns.get(2).name, is("col1"));
    }

    // TODO test storing and getting an object

    // TODO test creating a non existant table

    // TODO test creating a table with non existant column families

    // TODO test two tables over the same HTable wit overlapping columns

    // TODO test storing objects with all supported types of keys

    // TODO test storing an object with an unsupported type of key

}
