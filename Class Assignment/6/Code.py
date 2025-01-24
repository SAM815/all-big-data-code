# Given employees list
employees = [
    {"name": "Alice", "age": 30, "salary": 60000},
    {"name": "Bob", "age": 25, "salary": 50000},
    {"name": "Charlie", "age": 35, "salary": 75000},
    {"name": "David", "age": 40, "salary": 80000},
    {"name": "Eve", "age": 28, "salary": 45000},
    {"name": "Frank", "age": 33, "salary": 70000},
    {"name": "Grace", "age": 27, "salary": 55000},
    {"name": "Henry", "age": 45, "salary": 90000},
    {"name": "Ivy", "age": 32, "salary": 60000},
    {"name": "Jack", "age": 29, "salary": 65000}
]

# Calculate the total salary of all employees
total_salary = sum(map(lambda x: x["salary"], employees))

# Find the average age of employees
average_age = sum(map(lambda x: x["age"], employees)) / len(employees)

# Filter out employees who earn a salary greater than $50,000
filtered_employees = list(filter(lambda x: x["salary"] > 50000, employees))

# Sort the employees based on their salaries in descending order
sorted_employees = sorted(employees, key=lambda x: x["salary"], reverse=True)

# Print the results
print("Total Salary:", total_salary)
print("Average Age:", average_age)
print("Employees with Salary > $50,000:", filtered_employees)
print("Sorted Employees by Salary (Descending):", sorted_employees)
