# MiniDatabaseEngine
![alt text](https://docs-assets.developer.apple.com/published/183088f967/octree_2x_d5ec086e-6563-4f2b-99a2-4e1762919c72.png)
# Overview
In this project, you are going to build a small database engine with support for Octrees Indices. 
The required functionalities are 
1. Creating tables 
2. Inserting tuples 
3. Updating tuples
4. Deleting tuples 
5. Searching in tables Binary
6. Creating an Octree upon demand 
7. Using the created octree(s) where appropriate.

## Tables
1. Each table/relation will be stored as pages on disk (each page is a separate file). 
2. Supported type for a table’s column is one of: java.lang.Integer , java.lang.String , 
java.lang.Double , java.util.Date (Note: date acceptable format is "YYYY-MM-DD")
## Pages
1. A page has a predetermined fixed maximum number of rows (N). For example, if a 
table has 40000 tuples, and if N=200, the table will be stored in 200 files. 
2. You are required to use Java’s binary object file (.class) for emulating a page (to avoid 
having you work with file system pages, which is not the scope of this course). A single 
page must be stored as a serialized Vector (java.util.Vector) , because Vectors are thread 
safe). Note that you can save/load any Java object to/from disk by implementing the 
java.io.Serializable interface. You don’t actually need to add any code to your class to 
save it the hard disk.
3. A single tuple should be stored in a separate object inside the binary file. 
4. You need to postpone the loading of a page until the tuples in that page are actually 
needed. Note that the purpose of using pages is to avoid loading the entire table’s content 
into memory. Hence, it defeats the purpose to load all pages upon program startup. 
5. If all the rows in a page are deleted, then you are required to delete that page. Do not 
keep around completely empty pages. In the case of insert, if you are trying to insert in a 
full page, shift one row down to the following page. Do not create a new page unless you 
are in the last page of the table and that last one was full. 
6. You might find it useful to create a Table java class to store relevant information about 
the pages and serialize it just like you serialize a page.



* You will need to store the meta-data in a text file. This should have the following 
layout: 
**TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType, min, max**
  * **ClusteringKey** is set true if the column is the primary key. For simplicity, you will 
always sort the rows in the table according to the primary key. That’s why, it is also 
called the clusteringkey. Only 1 clustering key per table. 
  * **min** and **max** refer to the minimum and maximum values possible for that column. 
For example, if a user creates a table/relation CityShop, specifying several attributes 
with their types, etc… the file will be:

```
Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max 
CityShop, ID, java.lang.Integer, True, null, null, 0,10000 
CityShop, Name, java.lang.String, False, NameAddrSpecIndex, Octree, “A”, “ZZZZZZZZZZZ” 
CityShop, X, java.lang.Double, False, XYZIndex, Octree, 0,1000000 
CityShop, Y, java.util.Double, False, XYZIndex, Octree, 0,1000000 
CityShop, Z, java.lang.Double, False, XYZIndex, Octree, 0,1000000 
CityShop, Specialization, java.lang.String, False, NameAddrSpecIndex, Octree, “A”, “ZZZZZZZZZZZ” 
CityShop, Address, java.lang.String, False, NameAddrSpecIndex, Octree, “A”, “ZZZZZZZZZZZ”
```
*You must store the above metadata in a single file called **metadata.csv**. Do not worry 
about its size in your solution (i.e. do not page it). 

*You must use the metadata.csv file to learn about the types of the data being passed 
and verify it is of the correct type. So, do not treat metadata.csv as decoration!

## Indices
1. You are required to use an Octree to support creating indices. An Octree is on exactly 3 dimensions. You are not required to support other type of indices.
2. Each column has an associated range (minimum, maximum). You should use this range to create the divisions on the Index scale.
3.  You should update existing relevant indices when a tuple is inserted/deleted. 
4.  If an index is created after a table has been populated, you have no option but to scan the whole table to get the data read and inserted into the index.
5.  Upon application startup; to avoid having to scan all tables to build existing indices, you should save the index itself to disk and load it when the application starts next time.
6.  When a table is created, you do not need to create an index. An index will be created later on when the user requests that through a method call to createIndex
7.  Once an index exists, it should be used in executing queries where possible. Hence, if an index does not exist on columns that are being queried,**(e.g. select * from T where x = 20 and y= 30 and z = 20)**, then the query will be answered using linear scanning in T. However, if an index is created on the 3 columns queried, then the index should be used.
8. Note that indices should be used in answering multi-dimension partial queries if an index has been created on any of columns used in the query, e.g. **select * from Table1 where Table1.column1=value1 and Table1.column2=value2 and Table1.column3=value3 andTable1.column4=value4;** and an index has been created on Table1.column1, Table1.column2, and Table1.column3 then it should be used in answering the query.
9. Operator Inside SQLTerm can either be >, >=, <, <=, != or = 
10. Operator between SQLTerm (as in strarrOperators above) are **AND, OR, or XOR.**
11. DBAppException is a generic exception to avoid breaking the test cases when they run. You can customize the Exception by passing a different message upon creation. You should throw the exception whenever you are passed data you that will violate the integrity of your schema. 
12. SQLTerm is a class with 4 attributes: String _strTableName, String _strColumnName, String _strOperator and Object _objValue
13. Iterator is java.util.Iterator It is an interface that enables client code to iterate over the results row by row. Whatever object you return holding the result set, it should implement the Iterator interface.
14.You should check on the passed types and do not just accept any type – otherwise, your code will crash with invalid input. 
*Your main class should be called DBApp.java and should have the following seven methods with the signature as specified. The parameters names are written using Hungarian notation
```
public void init( ); 
public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException(){}
public void createIndex(String strTableName, String[] strarrColName) throws DBAppException(){}
public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException(){} 
public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException(){}
public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException(){}
public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException(){}
```
