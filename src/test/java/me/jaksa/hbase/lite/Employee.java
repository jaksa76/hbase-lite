package me.jaksa.hbase.lite;

import javax.persistence.*;
import java.io.Serializable;

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
