# hbase-lite
An abstraction layer on top of HBase that greatly simplifies writing of Hadoop M/R jobs.

# Tutorial

First you need to include the hbase-lite library in your pom file:

```xml
<dependency>
    <groupId>me.jaksa</groupId>
    <artifactId>hbase-lite</artifactId>
    <version>0.1.0</version>
</dependency>
```

```java
public class Demo {
    public static class Employee implements Serializable {
        private final Long id;
        private final String name;
        private final Integer salary;
        private final String department;
        private final String title;

        public Employee(Long id, String name, Integer salary, String department, String title) {
            this.id = id;
            this.name = name;
            this.salary = salary;
            this.department = department;
            this.title = title;
        }

        public Long getId() { return id; }
        public String getName() { return name; }
        public Integer getSalary() { return salary; }
        public String getDepartment() { return department; }
        public String getTitle() { return title; }
    }

    public static class EmployeeConverter implements Converter<Employee> {
        private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
        private static final byte[] NAME = Bytes.toBytes("name");
        private static final byte[] SALARY = Bytes.toBytes("sal");
        private static final byte[] DEPARTMENT = Bytes.toBytes("dep");
        private static final byte[] TITLE = Bytes.toBytes("title");

        @Override
        public Employee convert(Result result) {
            return new Employee(
                Bytes.toLong(result.getRow()),
                Bytes.toString(result.getValue(COLUMN_FAMILY, NAME)),
                Bytes.toInt(result.getValue(COLUMN_FAMILY, SALARY)),
                Bytes.toString(result.getValue(COLUMN_FAMILY, DEPARTMENT)),
                Bytes.toString(result.getValue(COLUMN_FAMILY, TITLE))
            );
        }

        @Override
        public Put toPut(Employee employee) {
            Put put = new Put(Bytes.toBytes(employee.getId()));
            put.addColumn(COLUMN_FAMILY, NAME, Bytes.toBytes(employee.getName()));
            put.addColumn(COLUMN_FAMILY, SALARY, Bytes.toBytes(employee.getSalary()));
            put.addColumn(COLUMN_FAMILY, DEPARTMENT, Bytes.toBytes(employee.getDepartment()));
            put.addColumn(COLUMN_FAMILY, TITLE, Bytes.toBytes(employee.getTitle()));
            return put;
        }
    }

    public static void main(String[] args) throws Exception {
        // to create a table supply the table name, the columns and the converter
        Table<Employee> employees = new Table<Employee>("employees", "cf:name,cf:sal,cf:dep,cf:title", new EmployeeConverter());

        // Let's first delete all objects from the table
        employees.deleteAll();

        // you can store, retrieve and delete objects from the table
        employees.put(new Employee(1l, "Joe", 30000, "HR", "Junior Recruiter"));
        Employee joe = employees.get(1l);
        System.out.println("Our first employee is: " + joe.getName());
        employees.delete(1l); // delete uses the id of the object, not the object itself

        // let's hire some more employees
        employees.put(new Employee(1l, "Joe", 30000, "SALES", "Junior Salesman"));
        employees.put(new Employee(2l, "Jane", 30000, "SW", "Junior Developer"));
        employees.put(new Employee(3l, "Jack", 100000, "SW", "Senior Manager"));
        employees.put(new Employee(4l, "Joan", 100000, "SW", "Senior Manager"));

        // you can run a reduce on a table and get the results immediately
        // notice that all the inputs are sent to the same group
        // I promise to implement some utility functions that will make it easier to do a sum
        int totalYrlCost = employees.reduce((Iterable<Employee> all) -> {
            int sum = 0;
            for (Employee employee : all) sum += employee.getSalary();
            return sum;
        });
        // also notice that the results are already available
        System.out.println("Total Yearly Cost: " + totalYrlCost);

        // of course you can use more compact forms of closures
        int count = employees.reduce(all -> Iterables.size(all));
        System.out.println("Number of employees: " + count);

        // you can do a map before the reduce which can be faster
        totalYrlCost = employees
                .map(employee -> employee.getSalary())
                .reduce(salaries -> {
                    int sum = 0;
                    for (int salary : salaries) sum += salary;
                    return sum;
                });
        System.out.println("Total Yearly Cost: " + totalYrlCost);

        // you can split the reduce into multiple groups using a partition
        // in Hadoop MR terms the partition determines the key of the mapper output
        // notice that in this case reduce returns an iterable
        // In future releases we will also be able to return a map so you get the name of the department as well
        Iterable<Integer> avgByDept = employees
                .partitionBy(employee -> employee.getDepartment())
                .map(employee -> employee.getSalary())
                .reduce(salaries -> {
                    int sum = 0;
                    for (int salary : salaries) sum += salary;
                    return sum;
                });
        System.out.println("Average salaries by dept: " + StringUtils.join(avgByDept.iterator(), ','));

        // you can chain map functions
        Integer salariesAfterBonus = employees
                .map(employee -> employee.getSalary())
                .map(salary -> salary * 115 / 100)
                .map(salary -> salary + 3000)
                .reduce(salaries -> {
                     int sum = 0;
                     for (double salary : salaries) sum += salary;
                     return sum;
                });
        System.out.println("Total cost after bonus: " + salariesAfterBonus);

        // you can specify several levels of partitioning
        Iterable<Integer> rolesByDept = employees
                .partitionBy(employee -> employee.getDepartment())
                .partitionBy(employee -> employee.getTitle().contains("Junior"))
                .reduce(all -> Iterables.size(all));
        System.out.println("Number of employees by role by department: " + StringUtils.join(rolesByDept.iterator(), ','));

        // and you can interleave map and partitionBy
        Iterable<Integer> salaryBandsByDeptAfterBonus = employees
                .partitionBy(employee -> employee.getDepartment())
                .map(employee -> employee.getSalary())
                .map(salary -> salary * 115 / 100)
                .map(salary -> salary + 3000)
                .partitionBy(salary -> Math.round(salary / 10000))
                .reduce(salaries -> Iterables.size(salaries)); // but there can be only one reduce and it will trigger the execution
        System.out.println("Number of salaries per band by department: " + StringUtils.join(salaryBandsByDeptAfterBonus.iterator(), ','));
    }
}
```

# Origins of hbase-lite

I have a PhD in Distributed Systems and have written several distributed computing
frameworks (even before Google's Map/Reduce). While I was working with Hadoop and HBase during a big data project
at Zuhlke I noticed that HBase and Hadoop in general are very hard to work with compared to other distributed
computing platforms, so I started writing some helper methods to provide a nicer API.
Although the initial implementation was and purpose specific I decided to write a generic library in
my free time that would allow everyone to easily write Map/Reduce jobs on top of HBase. The hbase-lite
library is still just a weekend project and doesn't have a dedicated team behind it.

