# BatchDB

A relational database management system which is designed for handling large datasets in low memory systems.

This document outlines the available commands for interacting with a simple database. 

### Commands

**Create:**

* Syntax: `create table [Table_name] {col1: data type1, col2: data type2};`
* Example: `create table user {id int, name str, points float};`
* Creates a new table with specified columns and data types.

**Insert:**

* Syntax: `insert into the table [Table_name] {value1, value2, ...};`
* Example: `insert into the table user {1, "user1", 3.92};`
* Inserts a new row into the specified table with provided values. Values must correspond to the order of columns defined in the table creation.

**Update:**

* Syntax: `update {col1: value1, col2: value2, ..} in the table [Table_name] where {conditions};`
* Example: `update {name: "xyz"} in the table user where {name == "user1" and id == 1};`
* Updates values of specified columns in a table where the provided conditions are met. Conditions are boolean expressions using operators like `==`, `>=`, etc.

**Delete:**

* Syntax: `delete from the table [Table_name] where {conditions};`
* Example: `delete from the table user where {name == "xyz" or id >= 10};`
* Deletes rows from a table where the provided conditions are met. Conditions are boolean expressions using operators like `==`, `>=`, etc.

**Drop:**

* Syntax: `drop table [Table_name];`
* Example: `drop table user;`
* Permanently deletes the specified table.

**Projections:**

* Syntax: `get [q1] in the table [q2] where [q3] groupby col having [q3] sortby [q4];`
* Optional clauses: groupby, having, sortby.
  * `groupby col`: Groups rows by the specified column.
  * `having [q3]`: Filters grouped rows based on the provided condition (not allowed without groupby).
  * `sortby [q4]`: Sorts the result by specified columns and order (`asc` or `desc`).

Some sample tables are already added for student and athlete to test example queries. You can also try similar queries with real world datasets: movies and credits.

**Examples:**

* `get {} in the table student;` (Gets all columns from the student table)
* `get {age, gpa} in the table student where {age >= 10 and gpa <= 3.5};` (Gets age and gpa columns for students with age >= 10 and gpa <= 3.5)
* `get {age, avg(gpa)} in the table student groupby age;` (Gets age and average gpa for each age group)
* `get {age, avg(gpa)} in the table student groupby age having {avg(gpa) >= 3.2};` (Gets age and average gpa for each age group where average gpa >= 3.2)
* `get {student.name, athlete.sport} in the table student joining athlete on student.id = athlete.id;` (Joins student and athlete tables on student.id and athlete.id, then gets name and sport)
* `get {} in the table student sortby {age, gpa} {desc, asc};` (Gets all columns from student table sorted by age (desc) and gpa (asc))
* `get {athlete.sport, avg(student.gpa)} in the table student joining athlete on student.id = athlete.id where {student.age >= 10} groupby athlete.sport having {avg(student.gpa) >= 3.2} sortby {athlete.sport, avg(student.gpa)} {asc, desc};` (Joins student and athlete tables, filters students with age >= 10, groups by sport in athlete table, filters groups with average gpa >= 3.2, gets sport and average gpa, then sorts by sport (asc) and average gpa (desc))

Finally, 

* To exit: `exit;`
