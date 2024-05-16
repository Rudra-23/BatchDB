# BatchDB

A relational database management system which is designed for handling large datasets in low memory systems.

This document outlines the available commands for interacting with a simple database.

To use the database, install required libraries via  `pip install -r requirements.txt`, then run the application using `python terminal.py`.

### Architecture

![Flow Diagram](https://github.com/Rudra-23/BatchDB/assets/54005905/67fb20c9-58d1-41f6-b481-421fa3d75f1a)

### Commands

**Create:**

* Syntax: `create table [Table_name] {col1 datatype1, col2 datatype2};`
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

### Examples

<img src="https://github.com/Rudra-23/BatchDB/assets/54005905/17a98221-7a86-4191-b59c-284fbb2cba27" width="80%">
<br>
<img src="https://github.com/Rudra-23/BatchDB/assets/54005905/f7351b4b-2b95-437c-8b21-bc0e31ff2ba4" width="80%">
<br>
<img src="https://github.com/Rudra-23/BatchDB/assets/54005905/ebad28bd-c68c-4936-9beb-a3ba22c42f00" width="80%">
<br>
<img src="https://github.com/Rudra-23/BatchDB/assets/54005905/00090d39-c714-4f63-911a-d4d21d02dd0c" width="80%">

### References:

* Pandas (nrows): <a href = "https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html">docs</a>
* Python's append: <a href = "https://stackoverflow.com/questions/65067610/does-pythons-append-file-write-mode-only-write-new-bytes-or-does-it-re-write#:~:text=The%20POSIX%20append%20mode%20simply,the%20end%20of%20the%20file.">stack-overflow</a>

Mem usage (when entire table is loaded directly in memory):

![image](https://github.com/Rudra-23/BatchDB/assets/54005905/d6cd7aa1-0781-4382-a3a5-46c0654d5788)

Mem usage (when same table is loaded using BatchDB):

![image](https://github.com/Rudra-23/BatchDB/assets/54005905/7f685596-da96-42ef-ad13-2bd9c713b3af)

