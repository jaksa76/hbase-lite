package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
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
import static me.jaksa.hbase.lite.TestUtils.*;

/**
* @author Jaksa Vuckovic
*/
public class TableTest {

    Converter<TestUtils.Dummy> converter = Mockito.mock(Converter.class);
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
        List<HColumn> columns = Table.extractColumns("fam1 :col1 , fam1: col2, fam2 : col1");

        assertThat(columns.size(), is(3));

        assertThat(columns.get(0), is(new HColumn("fam1", "col1")));
        assertThat(columns.get(1), is(new HColumn("fam1", "col2")));
        assertThat(columns.get(2), is(new HColumn("fam2", "col1")));
    }

    @Test
    public void testCreatingTable() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
    }

    @Test
    public void testGettingAnObject() throws Exception {
        when(hTable.get((Get) any())).thenReturn(DUMMY_RESULT);
        when(converter.convert(DUMMY_RESULT)).thenReturn(DUMMY_JOE);
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);

        TestUtils.Dummy joe = table.get("joe");

        assertEquals(DUMMY_JOE, joe);
    }

    @Test
    public void testStoringAnObject() throws Exception {
        when(converter.toPut(eq(DUMMY_JOE))).thenReturn(DUMMY_PUT);
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);

        table.put(DUMMY_JOE);

        verify(hTable).put(eq(DUMMY_PUT));
    }


    // TODO test creating a non existant table

    // TODO test creating a table with non existant column families

    // TODO test two tables over the same HTable wit overlapping columns

    @Test
    public void testSerializationOfStringKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get("StringKey");
        verify(hTable).get(argThat(getForKey(Bytes.toBytes("StringKey"))));
    }

    @Test
    public void testSerializationOfIntegerKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(1);
        verify(hTable).get(argThat(getForKey(Bytes.toBytes(1))));
    }

    @Test
    public void testSerializationOfLongKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(1l);
        verify(hTable).get(argThat(getForKey(Bytes.toBytes(1l))));
    }

    @Test
    public void testSerializationOfDoubleKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(1.0);
        verify(hTable).get(argThat(getForKey(Bytes.toBytes(1.0))));
    }

    @Test
    public void testSerializationOfFloatKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(1.0f);
        verify(hTable).get(argThat(getForKey(Bytes.toBytes(1.0f))));
    }

    @Test
    public void testSerializationOfShortKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get((short) 1);
        verify(hTable).get(argThat(getForKey(Bytes.toBytes(new Short((short) 1)))));
    }

    @Test
    public void testSerializationOfBigDecimalKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(new BigDecimal("1928734912837492183749.12394867192384792384"));
        verify(hTable).get(argThat(getForKey(Bytes.toBytes(new BigDecimal("1928734912837492183749.12394867192384792384")))));
    }

    @Test
    public void testSerializationOfBooleanKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(true);
        verify(hTable).get(argThat(getForKey(Bytes.toBytes(new Boolean(true)))));
    }

    @Test
    public void testSerializationOfByteBufferKeys() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(ByteBuffer.wrap("abc".getBytes()));
        verify(hTable).get(argThat(getForKey("abc".getBytes())));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerializationOfUnsupportedKeyType() throws Exception {
        Table<TestUtils.Dummy> table = new Table(hTable, "fam1:col1,fam1:col2,fam2:col1", converter);
        table.get(Runtime.getRuntime());
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
