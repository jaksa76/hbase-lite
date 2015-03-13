package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class GenericConverterTest {

    public static final byte[] CF = toBytes("cf");
    public static final byte[] EXT = toBytes("ext");
    public static final byte[] NAME = toBytes("name");
    public static final byte[] TITLE = toBytes("title");
    public static final byte[] SALARY = toBytes("sal");
    public static final byte[] DEPARTMENT = toBytes("dpt");

    @Test
    public void testConvertingEmployeeToPut() throws Exception {
        GenericConverter<Employee> converter = new GenericConverter(Employee.class);
        Employee joe = new Employee(01L, "Joe", 30000.0, "HR", "Junior Recruiter");

        Put put = converter.toPut(joe);

        assertArrayEquals(put.getRow(), toBytes(01L));
        assertTrue(put.has(CF, NAME, toBytes("Joe")));
        assertTrue(put.has(CF, TITLE, toBytes("Junior Recruiter")));
        assertTrue(put.has(CF, SALARY, toBytes(30000.0)));
        assertTrue(put.has(EXT, DEPARTMENT, toBytes("HR")));
    }

    @Test
    public void testConvertingFromResult() throws Exception {
        Result result = Mockito.mock(Result.class);
        when(result.getRow()).thenReturn(toBytes(01L));
        when(result.getValue(CF, NAME)).thenReturn(toBytes("Joe"));
        when(result.getValue(CF, TITLE)).thenReturn(toBytes("Junior Recruiter"));
        when(result.getValue(CF, SALARY)).thenReturn(toBytes(30000.0));
        when(result.getValue(EXT, DEPARTMENT)).thenReturn(toBytes("HR"));

        GenericConverter<Employee> converter = new GenericConverter(Employee.class);
        Employee joe = converter.convert(result);

        assertEquals(new Long(01L), joe.getId());
        assertEquals("Joe", joe.getName());
        assertEquals("Junior Recruiter", joe.getTitle());
        assertEquals(new Double(30000.0), joe.getSalary());
        assertEquals("HR", joe.getDepartment());
    }
}