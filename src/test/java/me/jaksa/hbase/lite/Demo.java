package me.jaksa.hbase.lite;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author Jaksa Vuckovic
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        // to create a table supply the element class
        Table<Employee> employees = new Table<Employee>(Employee.class);

        // Let's first delete all objects from the table
        employees.deleteAll();

        // you can store, retrieve and delete objects from the table
        employees.put(new Employee(1l, "Joe", 30000.0, "HR", "Junior Recruiter"));
        Employee joe = employees.get(1l);
        System.out.println("Our first employee is: " + joe.getName());
        employees.delete(1l); // delete uses the id of the object, not the object itself

        // let's hire some more employees
        employees.put(new Employee(1l, "Joe", 30000.0, "SALES", "Junior Salesman"));
        employees.put(new Employee(2l, "Jane", 30000.0, "SW", "Junior Developer"));
        employees.put(new Employee(3l, "Jack", 100000.0, "SW", "Senior Manager"));
        employees.put(new Employee(4l, "Joan", 100000.0, "SW", "Senior Manager"));

        // you can run a reduce on a table and get the results immediately
        // notice that all the inputs are sent to the same group
        // I promise to implement some utility functions that will make it easier to do a sum
        double totalYrlCost = employees.reduce((Iterable<Employee> all) -> {
            double sum = 0;
            for (Employee employee : all) sum += employee.getSalary();
            return sum;
        });
        // also notice that the results are already available
        System.out.println("Total Yearly Cost: " + totalYrlCost);

        // of course you can use more compact forms of closures
        long count = employees.reduce(all -> Stats.count(all));
        System.out.println("Number of employees: " + count);

        // you can do a map before the reduce which can be faster
        totalYrlCost = employees
                .map(employee -> employee.getSalary())
                .reduce(salaries -> Stats.sum(salaries));
        System.out.println("Total Yearly Cost: " + totalYrlCost);

        // you can split the reduce into multiple groups using a partition
        // in Hadoop MR terms the partition determines the key of the mapper output
        // notice that in this case reduce returns a Map that holds the reduce value for
        // each partition key
        Map<String, Double> avgByDept = employees
                .partitionBy(employee -> employee.getDepartment())
                .map(employee -> employee.getSalary())
                .reduce(salaries -> Stats.sum(salaries));
        System.out.println("Average salaries by dept: ");
        System.out.println(avgByDept);

        // you can chain map functions
        Double salariesAfterBonus = employees
                .map(employee -> employee.getSalary())
                .map(salary -> salary * 1.15)
                .map(salary -> salary + 3000.0)
                .reduce(salaries -> Stats.sum(salaries));
        System.out.println("Total cost after bonus: " + salariesAfterBonus);

        // you can specify several levels of partitioning
        // the key of the map will be a list of keys that you used to partition
        // for each combination of keys that generated a non empty partition there will be a map entry
        Map<List, Long> rolesByDept = employees
                .partitionBy(employee -> employee.getDepartment())
                .partitionBy(employee -> employee.getTitle().contains("Junior"))
                .reduce(all -> Stats.count(all));
        System.out.println("Number of employees by role by department: ");
        System.out.println(rolesByDept);

        // and you can interleave map and partitionBy
        Map<List, Long> salaryBandsByDeptAfterBonus = employees
                .partitionBy(employee -> employee.getDepartment())
                .map(employee -> employee.getSalary())
                .map(salary -> salary * 115 / 100)
                .map(salary -> salary + 3000)
                .partitionBy(salary -> Math.round(salary / 10000))
                .reduce(salaries -> Stats.count(salaries)); // but there can be only one reduce and it will trigger the execution
        System.out.println("Number of salaries per band by department: ");
        System.out.println(salaryBandsByDeptAfterBonus);
    }
}
