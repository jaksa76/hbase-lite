package me.jaksa.hbase.lite;

import com.google.common.collect.Iterables;
import me.jaksa.hbase.lite.TestUtils.Dummy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.google.common.collect.Iterables.size;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Jaksa Vuckovic
 */
public class TableIntegrationTest {

    public static final String KEY = "a dummy test";
    private Table<Dummy> testTable;

    @Before
    public void setUp() throws IOException {
        TestUtils.createTestTable();
        testTable = new Table("test-table", "cf:value,cf:val", new TestUtils.DummyConverter());
        testTable.deleteAll();
    }

    @Test
    public void testStoringAndGettingSomeObjects() throws Exception {
        Assert.assertNull(testTable.get(KEY));

        testTable.put(new Dummy(KEY, "one"));

        Assert.assertEquals("one", testTable.get(KEY).value);
    }

    @Test
    public void testReducing() throws Exception {
        testTable.put(new Dummy("jack", "two"));
        testTable.put(new Dummy("jill", "three"));
        testTable.put(new Dummy("june", "four"));

        assertThat(testTable.reduce(values -> size(values)), is(3));
    }

    @Test
    public void testReducingEmptyTable() throws Exception {
        assertThat(testTable.reduce(values -> size(values)), is(0));
    }

}
