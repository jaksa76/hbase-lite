package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * Represents an HBase column.
 *
 * @author Jaksa Vuckovic
*/
class HColumn {
    public final byte[] family;
    public final byte[] name;

    public HColumn(@Nonnull String family, @Nonnull String name) {
        this.family = Bytes.toBytes(family);
        this.name = Bytes.toBytes(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HColumn column = (HColumn) o;

        if (!Arrays.equals(family, column.family)) return false;
        if (!Arrays.equals(name, column.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(family);
        result = 31 * result + Arrays.hashCode(name);
        return result;
    }
}
