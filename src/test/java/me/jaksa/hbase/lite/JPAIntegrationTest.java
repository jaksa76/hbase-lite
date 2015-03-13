package me.jaksa.hbase.lite;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Jaksa Vuckovic
 */
public class JPAIntegrationTest {
    private Table<Employee> employees;

    @Before
    public void setUp() throws IOException {
        TestUtils.createEmployeeTable();
        employees = new Table(Employee.class);
        employees.deleteAll();
    }

    @Test
    public void testStoringAndGettingSomeObjects() throws Exception {
        Long key = 1L;
        Assert.assertNull(employees.get(key));

        employees.put(new Employee(key, "Joe", 30000.0, "HR", "Junior Recruiter"));

        assertEquals(key, employees.get(key).getId());
        assertEquals("Joe", employees.get(key).getName());
        assertEquals(new Double(30000.0), employees.get(key).getSalary());
        assertEquals("HR", employees.get(key).getDepartment());
        assertEquals("Junior Recruiter", employees.get(key).getTitle());
    }

    @Test
    public void testMapReduce() throws Exception {
        employees.put(new Employee(1l, "Joe", 30000.0, "SALES", "Junior Salesman"));
        employees.put(new Employee(2l, "Jane", 30000.0, "SW", "Junior Developer"));
        employees.put(new Employee(3l, "Jack", 100000.0, "SW", "Senior Manager"));
        employees.put(new Employee(4l, "Joan", 100000.0, "SW", "Senior Manager"));

        List<Long> results = Lists.newArrayList(employees
                .partitionBy(employee -> employee.getDepartment())
                .map(employee -> employee.getSalary())
                .map(salary -> salary * 115 / 100)
                .map(salary -> salary + 3000)
                .partitionBy(salary -> Math.round(salary / 10000))
                .reduce(salaries -> Stats.count(salaries)));

        assertThat(results.size(), is(3));
        assertThat(results.get(0), is(1L));
        assertThat(results.get(1), is(2L));
        assertThat(results.get(2), is(1L));
    }
}
