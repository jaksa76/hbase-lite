package me.jaksa.hbase.lite;

import org.junit.Assert;
import org.junit.Test;

import javax.persistence.*;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;

public class JPAUtilsTest {

    @Test
    public void testGettingTableNameFromAnnotation() throws Exception {
        @Entity
        @javax.persistence.Table(name="my-table")
        class MyClass {
            @Id private Long id;
            public Long getId() { return id; }
            public void setId(Long id) { this.id = id; }
        }

        Assert.assertThat(JPAUtils.getTableName(MyClass.class), is("my-table"));
    }

    @Test
    public void testGettingTableNameFromClassName() throws Exception {
        @Entity
        class Employee {
            @Id private Long id;
            public Long getId() { return id; }
            public void setId(Long id) { this.id = id; }
        }

        Assert.assertThat(JPAUtils.getTableName(Employee.class), is("Employees"));
    }

    @Test
    public void testGettingTableNameFromClassNameWithEndingS() throws Exception {
        class MyClass {
            @Id private Long id;
            public Long getId() { return id; }
            public void setId(Long id) { this.id = id; }
        }

        Assert.assertThat(JPAUtils.getTableName(MyClass.class), is("MyClasses"));
    }

    @Test
    public void testGettingColumns() throws Exception {
        Assert.assertThat(JPAUtils.getColumns(Employee.class).values(), hasItems(
                new HColumn("cf","name"),
                new HColumn("cf","title"),
                new HColumn("cf","sal"),
                new HColumn("ext","dpt")
        ));
    }

    @Test
    public void testGettingKey() throws Exception {
        Field keyField = JPAUtils.getKeyField(Employee.class);
        Assert.assertThat(keyField.getName(), is("id"));
        Assert.assertThat(keyField.getType(), equalTo(Long.class));
    }
}
