# FDW development demo
This is a minimal fdw implementation to invoke external table(gpfdist). Main purpose is to demo FDW development process. 

# FDW source organization
The basic fdw directory should contain these files.
```
*.c
Makefile
xxx.control
xxx--1.0.sql
```
### Makefile
[Detail document](https://www.postgresql.org/docs/current/extend-pgxs.html). Can be copied from contrib/file_fdw. Key contents:
|||
|---|---|
|MODULE|Name for .so file, if OBJ is not specified same .c file is used to compile|
|MODULE_big|Name for .so file if there are multiple source files|
|OBJ|List of object files(.o), the correspond .c files are compiled|
|EXTENSION|extension name|
|DATA|A sql script file for CREATE EXTENSION.|

### FDW entry point
- ext_fdw_handler

Main entry point. Returns a struct that contains a set of function pointers.

For SELECT queries, implement
```
	GetForeignRelSize_function GetForeignRelSize;
	GetForeignPaths_function GetForeignPaths;
	GetForeignPlan_function GetForeignPlan;
	BeginForeignScan_function BeginForeignScan;
	IterateForeignScan_function IterateForeignScan;
	ReScanForeignScan_function ReScanForeignScan;
	EndForeignScan_function EndForeignScan;
```
For INSERT queries, implement
```
	PlanForeignModify_function PlanForeignModify;
	BeginForeignModify_function BeginForeignModify;
	EndForeignModify_function EndForeignModify;
	BeginForeignInsert_function BeginForeignInsert;
    ExecForeignInsert_function ExecForeignInsert;
	EndForeignInsert_function EndForeignInsert;
```

- ext_fdw_validator

Called when ```CREATE SERVER``` and ```CREATE FOREIGN TABLE```. 

### Interesting functions
- GetForeignPaths: create "path node" for scan plan. For FDW usually there's only one path node. We can specify the "locus" for a path node, which means how data is distributed when doing scan and can affect whether FDW is executed on segments. For example:
```
CdbPathLocus_MakeStrewn(&pathnode->path.locus, getgpsegmentCount());
```
This will set the locus to "Strewn", that is randomly distributed. We can also call ```CdbPathLocus_MakeEntry``` for master only and ```CdbPathLocus_MakeHashed``` for hash distributed.

This is also the point to construct a generic struct node from FDW server & table options, for other FDW fucntions to consume. Pass this as the last parameter to ```create_foreignscan_path```  **There's a problem here for execute on segments**, this struct must be serialized and deserialized between QD and segments, so there must be a plan node and copy functions(nodes.h, copyfuncs.c) for it **in kernel**, because these functions are executed before FDW is loaded. Search ExternalScanInfo for example. So we can only use String node here, not a generic struct.

- BeginForeignScan, EndForeignScan: Do init/cleanup work in these functions. They will be called on master AND on segments. One important note: 

- IterateForeignScan: This is the real worker function for SELECT. Construct one tuple each time. Usually a tuple is constructed from array of values and nulls. If the predefined array in ```node->ss.ss_ScanTupleSlot``` is used, call ```ExecStoreVirtualTuple```. Otherwise call ```ExecStoreHeapTuple``` to directly set the tuple.
If EOF is reached, call ```ExecClearTuple```.

- BeginForeignModify/EndForeignModify: Same as fucntions for SELECT, these are the points to do init/cleanup for INSERT.

- ExecForeignInsert: Worker function for INSERT. Get the tuple from ```slot``` and then do the rest of work.


## Example scripts
```
CREATE EXTENSION ext_fdw;
CREATE SERVER ext_s FOREIGN DATA WRAPPER ext_fdw;
CREATE FOREIGN TABLE fdw_tbl(a int) SERVER ext_s OPTIONS(location 'gpfdist://127.0.0.1:9090/c.csv', format 'csv');
SELECT count(*) FROM fdw_tbl;

CREATE TABLE t(a int);
INSERT INTO t SELECT generate_series(1,100);
INSERT INTO fdw_tbl SELECT * FROM t;
```