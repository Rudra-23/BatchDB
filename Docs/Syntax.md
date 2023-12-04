# BatchDB

A relational database management system which is designed for handling large datasets in low memory systems.

This document outlines the available commands for interacting with a simple database.

To use the database, install required libraries via  `pip install -r requirements.txt`, then run the application using `python terminal.py`.

### Commands

**Create:**

* Syntax: `create table [Table_name] {col1: data type1, col2: data type2};`
* Example: `create table user {id int, name str, points float};`
* Creates a new table with specified columns and data types.

**Insert:**

* Syntax: `insert into the table [Table_name] {value1, value2, ...};`
* Example: `insert into the table user {1, "user1", 3.92};`
* Inserts a new row into the specified table with provided values. Values must correspond to the order of columns defined in the table creation.

To check the table, use: `get {} in the table user;`

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

Some real world datasets are already added for movies and credits to test example queries. You can also try similar queries with small datasets: student and athlete.

**Examples:**

* `get {} in the table movies;`
* `get {} in the table movies where {runtime >= 180 and genre != 'Action'};`
* `get {genre, count(genre), avg(runtime)} in the table movies groupby genre;`
* `get {genre, count(genre), avg(runtime)} in the table movies groupby genre having {avg(runtime) >= 120};`
* `get {} in the table movies joining credits on movies.id = credits.movie_id;`
* `get {} in the table movies sortby {genre, runtime} {desc, asc};`
* `get {movies.genre, avg(credits.cast_size), max(movies.runtime)} in the table movies joining credits on movies.id = credits.movie_id where {movies.runtime >= 120} groupby movies.genre having {avg(credits.cast_size) >= 30} sortby {movies.genre} {desc};`

Finally,

* To exit: `exit;`

File structure:

* The directory Data, TMP and metadata.json is about storing and processing the data.
* DDL directory contains files about create and drop table commands.
* DML directory contains files about insert, update and delete rows.
* Operators contains python files implementing operators like join, sort, groupby etc.
* QueryParser does query execution and manages complex queries.
* terminal.py is the terminal to input queries and output results.
* Docs contains syntax of query language.
* Rests are utility files for maintaining the project.
