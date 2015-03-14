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

You can use JPA annotations to specify how the class will be stored. If you can't modify your domain objects, you can implement your own Converter.
Here is an example of a JPA mapping:

```java
@Entity
// the default table name is the plural of the class name (e.g. Employee -> Employees, MyClass -> MyClasses)
//@Table(name = "my-employee-table") // we can explicitly specify the table name here
public class Employee implements Serializable {
    @Id
    private Long id;

    // the default column family is "cf", so name will be stored in cf:name
    @Column
    private String name;

    // just like in JPA you don't need to annotate all fields, they will get picked up anyway
    private String title;

    // if you want you can specify a different column name
    @Column(name = "sal")
    private Double salary;

    // you can also specify the column family
    @Column(name = "ext:dpt")
    private String department;

    public Employee() { }

    public Employee(Long id, String name, Double salary, String department, String title) {
        this.id = id;
        this.name = name;
        this.salary = salary;
        this.department = department;
        this.title = title;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Double getSalary() {
        return salary;
    }

    public String getDepartment() {
        return department;
    }

    public String getTitle() {
        return title;
    }
}
```



```java
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
```

# Spark VS HBaseLite

HBaseLite and Spark have a very similar API, but HBaseLite is not meant to be a replacement for Spark. Rather than that
it is a way to achieve some of the benefits of Spark while still using HBase and Hadoop Map/Reduce underneath.
It is also the ideal intermediate step between Hadoop and Spark.


# Origins of hbase-lite

I have a PhD in Distributed Systems and have written several distributed computing
frameworks in the early 2000s. While I was working with Hadoop and HBase during a big data project
at Zuhlke I had the need for a nicer API. Although the initial implementation was and purpose specific I decided
to write a generic library in my free time that would allow everyone to easily write Map/Reduce jobs on top of HBase.
The hbase-lite library is still just a weekend project and doesn't have a dedicated team behind it.
