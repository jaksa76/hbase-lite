package me.jaksa.hbase.lite;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

public class StatsTest {
    private static final Iterable<Integer> INTS = asList(1, 2, 3, 4, 5, 6);
    private static final Iterable<Long> LONGS = asList(1l, 2l, 3l, 4l, 5l, 6l);
    private static final Iterable<Float> FLOATS = asList(1.1f, 2.1f, 3.1f, 4.1f, 5.1f, 6.1f);
    private static final Iterable<Double> DOUBLES = asList(1.1, 2.1, 3.1, 4.1, 5.1, 6.1);

    @Test
    public void testSum() throws Exception {
        assertThat(Stats.sumInts(INTS), is(21l));
        assertThat(Stats.sumLongs(LONGS), is(21l));
        assertThat(Stats.sumFloats(FLOATS), is(closeTo(21.6, 1E-5)));
        assertThat(Stats.sum(DOUBLES), is(closeTo(21.6, 1E-10)));
    }

    @Test
    public void testCount() throws Exception {
        assertThat(Stats.count(DOUBLES), is(6l));
    }
}