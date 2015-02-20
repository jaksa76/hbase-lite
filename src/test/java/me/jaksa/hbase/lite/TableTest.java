package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableTest {
    public static final Dummy DUMMY_JOE = new Dummy("joe", "1");
    public static final Put DUMMY_PUT = new Put(new byte[1]);
    public static final Result DUMMY_RESULT = new Result();

    Converter<Dummy> converter = Mockito.mock(Converter.class);
    private final HTable hTable = Mockito.mock(HTable.class);

    @Before
    public void setUp() {
        when(hTable.getName()).thenReturn(TableName.valueOf("myTable"));
    }

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

    @Test
    public void testCreatingTable() throws Exception {
        Table<Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
    }

    @Test
    public void testGettingAnObject() throws Exception {
        when(hTable.get((Get) any())).thenReturn(DUMMY_RESULT);
        when(converter.convert(DUMMY_RESULT)).thenReturn(DUMMY_JOE);
        Table<Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);

        Dummy joe = table.get("joe");

        assertEquals(DUMMY_JOE, joe);
    }

    @Test
    public void testStoringAnObject() throws Exception {
        when(converter.toPut(eq(DUMMY_JOE))).thenReturn(DUMMY_PUT);
        Table<Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);

        table.put(DUMMY_JOE);

        verify(hTable).put(eq(DUMMY_PUT));
    }


    // TODO test creating a non existant table

    // TODO test creating a table with non existant column families

    // TODO test two tables over the same HTable wit overlapping columns


    // TODO test storing objects with all supported types of keys
    @Test
    public void testSerializationOfKeys() throws Exception {
        Table<Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get("StringKey");
        verify(hTable).get(argThat(getForKey(Bytes.toBytes("StringKey"))));
    }


    // TODO test storing an object with an unsupported type of key


    static class Dummy {
        public final String name;
        public final String value;
        Dummy(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }

    static Matcher<Get> getForKey(byte[] key) {
        return new TypeSafeMatcher<Get>() {
            @Override
            protected boolean matchesSafely(Get get) {
                return Arrays.equals(get.getRow(), key);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(key.toString());
            }
        };
    }
}
