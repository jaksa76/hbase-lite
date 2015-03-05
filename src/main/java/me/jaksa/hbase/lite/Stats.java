package me.jaksa.hbase.lite;

/**
 * Some statistical utility methods.
 *
 * @author Jaksa Vuckovic
 */
public class Stats {
    public static Long sumInts(Iterable<? extends Integer> numbers) {
        long sum = 0;
        for (Integer n : numbers) sum += n;
        return sum;
    }

    public static Long sumLongs(Iterable<? extends Long> numbers) {
        Long sum = 0l;
        for (Long n : numbers) sum += n;
        return sum;
    }

    public static Double sumFloats(Iterable<? extends Float> numbers) {
        double sum = 0;
        for (Float n : numbers) sum += n;
        return sum;
    }

    public static Double sum(Iterable<? extends Double> numbers) {
        double sum = 0;
        for (Double n : numbers) sum += n;
        return sum;
    }

    public static Long count(Iterable iterable) {
        long count = 0;
        for (Object o : iterable) count++;
        return count;
    }
}
