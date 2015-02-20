package me.jaksa.hbase.lite;

import me.jaksa.hbase.lite.TestUtils.Dummy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Jaksa Vuckovic
 */
public class TableIntegrationTest {

    public static final String KEY = "a dummy test";

    @Before
    public void setUp() throws IOException {
        TestUtils.createTestTable();
    }

    @Test
    public void testStoringAndGettingSomeObjects() throws Exception {
        Table<Dummy> testTable = new Table("test-table", "cf:value,cf:val", new TestUtils.DummyConverter());

        testTable.delete(KEY);
        Assert.assertNull(testTable.get(KEY));

        testTable.put(new Dummy(KEY, "one"));

        Assert.assertEquals("one", testTable.get(KEY).value);
    }
}
