package me.jaksa.hbase.lite;

import com.google.common.collect.Lists;
import me.jaksa.hbase.lite.TestUtils.Dummy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import static com.google.common.collect.Iterables.size;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
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
    public void testMapReduce() throws Exception {
        testTable.put(new Dummy("jack", "two"));
        testTable.put(new Dummy("jill", "three"));
        testTable.put(new Dummy("june", "four"));

        List<String> result = testTable.map(d -> d.value).reduce(values -> Lists.newArrayList(values));

        assertThat(result, hasItems("two", "three", "four"));
    }


    @Test
    public void testPartitioning() throws Exception {
        testTable.put(new Dummy("jack", "two"));
        testTable.put(new Dummy("jill", "three"));
        testTable.put(new Dummy("june", "four"));

        Map<Character, Integer> result = testTable
                .map(d -> d.value)
                .partitionBy(v -> v.charAt(0))
                .reduce(values -> size(values));

        assertThat(result.size(), is(2));
        assertEquals((char) result.keySet().iterator().next(), 'f');
        assertThat(result.get('t'), is(2)); // two and three
        assertThat(result.get('f'), is(1)); // four
    }

    @Test
    public void testChainedMapSteps() throws Exception {
        testTable.put(new Dummy("jack", "2"));
        testTable.put(new Dummy("jill", "3"));
        testTable.put(new Dummy("june", "4"));

        List<Integer> result = testTable
                .map(d -> d.value)
                .map(v -> Integer.parseInt(v))
                .map(n -> n - 1)
                .reduce(values -> Lists.newArrayList(values));

        assertThat(result, hasItems(1, 2, 3));
    }

    @Test
    public void testMultiPartitioning() throws Exception {
        testTable.put(new Dummy("joe",  "15"));
        testTable.put(new Dummy("jack", "10"));
        testTable.put(new Dummy("jill", "21"));
        testTable.put(new Dummy("june", "22"));
        testTable.put(new Dummy("tom",  "32"));

        Map<List, Long> result = testTable
                .partitionBy(d -> d.name.charAt(0))
                .partitionBy(v -> v.name.length())
                .map(d -> d.value)
                .map(v -> Integer.parseInt(v))
                .reduce(values -> Stats.sumInts(values));

        assertThat(result.size(), is(3));
        assertThat(result.get(asList('j', 3)), equalTo(15L));
        assertThat(result.get(asList('j', 4)), equalTo(53L));
        assertThat(result.get(asList('t', 3)), equalTo(32L));
    }

    @Test
    public void testReducingEmptyTable() throws Exception {
        assertThat(testTable.reduce(values -> size(values)), is(0));
    }
}
