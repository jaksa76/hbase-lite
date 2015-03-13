package me.jaksa.hbase.lite;

import javax.persistence.*;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for the JPA mapping.
 */
public class JPAUtils {
    public static String getTableName(Class clazz) {
        javax.persistence.Table entity = (javax.persistence.Table) clazz.getAnnotation(javax.persistence.Table.class);
        if (entity != null) {
            String name = entity.name();
            if (name != null && !name.isEmpty()) return name;
        }

        // if not specified in annotation, use the class name
        String simpleName = clazz.getSimpleName();
        return simpleName.endsWith("s") ? simpleName + "es" : simpleName + "s";
    }

    public static Map<Field, HColumn> getColumns(Class clazz) {
        Map<Field, HColumn> fields = new HashMap<>();
        for (Field f : clazz.getDeclaredFields()) {
            if (!isIdentifier(f)) {
                fields.put(f, new HColumn(getColumnFamily(f), getColumnName(f)));
            }
        }
        return fields;
    }

    public static <T> Field getKeyField(Class<T> clazz) {
        for (Field f : clazz.getDeclaredFields()) {
            if (isIdentifier(f)) return f;
        }
        throw new RuntimeException("Class " + clazz.getName() + " doesn't have a primary key. " +
                "Please annotate a field with javax.persistence.Id.");
    }

    private static boolean isIdentifier(Field f) {
        return f.getAnnotation(Id.class) != null;
    }

    private static String getColumnName(Field f) {
        javax.persistence.Column column = f.getAnnotation(javax.persistence.Column.class);
        if (column != null) {
            String name = column.name();
            if (!name.isEmpty()) {
                String[] parts = name.split(":");
                if (parts.length == 2) return parts[1];
                else return parts[0];
            }
        }
        return f.getName();
    }

    private static String getColumnFamily(Field f) {
        javax.persistence.Column column = f.getAnnotation(javax.persistence.Column.class);
        if (column != null) {
            String name = column.name();
            if (!name.isEmpty()) {
                String[] parts = name.split(":");
                if (parts.length == 2) return parts[0];
            }
        }
        return "cf";
    }
}
