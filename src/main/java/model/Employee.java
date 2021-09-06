package model;


import java.util.Date;

public class Employee {
    private int employeeId;
    private String employeeName;
    private Date employeeJoinDate;

    public Employee(int employeeId, String employeeName, Date employeeJoinDate) {
        this.employeeId = employeeId;
        this.employeeName = employeeName;
        this.employeeJoinDate = employeeJoinDate;
    }

    public int getEmployeeId() {
        return employeeId;
    }

    public String getEmployeeName() {
        return employeeName;
    }

    public Date getEmployeeJoinDate() {
        return employeeJoinDate;
    }

}