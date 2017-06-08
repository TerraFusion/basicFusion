
# File MetaData DB Build and Query

## Overview
File metadata from the BW nearline storage is parsed and stored in a sqlite database by the **_fusionBuildDB_** python program.  Use *fusionBuildDB --help* to see the full array of options and arguments.

   The provided *fusionQuery* python program uses the python sqlite interface to query the database, but is not required.  Queries can be executed directly using the sqlite binary by passing the database and query, and this method is typically easier than modifiying the fusionQuery script.

## Workflow

1. The file listings for all instruments are gathered from the BW storage team and arranged as a set of compressed text files.  The orbit-path-time file should also be available. Examples in the source tree are available under data (full-data) and testdata(single orbit and synthetic data for testing):  
* Orbit_Path_Time.txt.gz, MODIS.list.gz, ASTER.list.gz, MOPITT.list.gz, MISR.list.gz, CERES.list.gz
2. The database is built with the *fusionBuildDB* program by passing these files and other options to the fusionBbuildDB program.
3. The database can be copied as needed to locations where queries are required.
4. The database can be queried as needed, example queries are shown below.

## Conventions
- Times:  
  - All times are specified as seconds since 00:00:00, Jan 1, 1970.
  - See query examples below for converting date-strings to seconds since the epoch.

## Database Schema
The database includes the following tables and columns.   
- orbits:
  - orbit-num (int)
  - path-num (int)
  - start-time (int)
  - end-time (int)
- aster, ceres, miser, modis, mopitt:
  - start-time (int)
  - end-time (int)
  - directory-id (int)
  - file-name (char)
- dirs:
  - directory-id (int)
  - directory-path (char)

## File-Name Metadata Formats and Time Extraction
Note:  directory-paths not shown.

__Orbit_Path_time__ 

```
85297 015 2015-12-31T15:25:54Z 2015-12-31T17:04:47Z
NNNNN PPP YYYY-MM-DD HH MM SS  YYYY MM DD HH MM SS
^^^^^ ^^^ ^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^
Orbit Path    start-Time           end-time
```
__MODIS:__
```
MOD021KM.A2006365.2315.006.2014221124537.hdf
          YYYYDDD HHMM
          ^^^^^^^ ^^^^
           start-time     end-time = start-time + 5*60 (5 min)
```
__MISR:__
```
MISR_AM1_GRP_ELLIPSOID_GM_P084_O052725_DF_F03_0024.hdf
                                OOOOOO
                                ^^^^^^
                                orbit#

    start-time = start_time(orbit#)  end_time = end_time(orbit)
```
__ASTER:__
```
AST_L1T_00301032002165035_20150423183015_59599.hdf
           DDMMYYYYHHMMSS
           ^^^^^^^^^^^^^^
             start-time           end-time = start_time + 9 (9 sec)
```
__CERES:__
```
CER_SSF_Terra-FM2_Edition4_400403.2007070205
                                  YYYYMMDDMM
                                  ^^^^^^^^^^
                                 start-time
    end-time = start-time + 24*60*60 (24 hr)
```
 __MOPITT:__
 ```
 MOP01-20021129-L1V3.50.0.he5
       YYYYMMDD
       ^^^^^^^^
       start-time
   end-time = start-time + 24*60*60 (24 hrh)
 ```
  MOP01-20070703-L1V3.50.0.he5
 * start:        YYYYMMDD

## Discards
Data that does not match the above metadata formats or immediately preceeding directory-path notations are discarded.   See the   HEADFILTERS and TAILFILTERS definitions in fusionBuildDB for details.
Discards can be captured during database build with the --discards option,see fusionBuildDB --help for details.

## Build Examples
See .vscode/launch.json for the various run configurations used to build under vscode.  These examples are taken from there:
- Full Build
```
fusionBuildDB -o -q --discards=discards.txt --anomalies=errors.txt accesslist.sqlite data/Orbit_Path_Time.txt.gz data/MODIS.list.gz data/ASTER.list.gz data/MOPITT.list.gz data/MISR.list.gz data/CERES.list.gz
```
- One-orbit test Build
```
fusionBuildDB -o -v --discards=discards.txt --trace=trace.txt --anomalies=errors.txt oneorbit.sqlite data/Orbit_Path_Time.txt.gz testdata/one-orbit-files.txt.gz
```
## Query Support for Scripts
With the start and end time of all files in the DB many useful queries can be constructed in sql.   The BETWEEN operator can find times that lie within a particular span, and overlapping timespans can be selected on 2 ranges with start1/end1 and start2/end2 by selecting on *start1 <= end2 and end1 >= start2*

### Command-line and Script Support
The file queries.bash includes a number or reusable query functions for bash.
These include by orbit, and by instrument/orbit. Use these by inclusion in any bash script or source them directly into another bash script with:
```
. ./queries.bash
```
Some of the queries available in queries.bash:
- files overlapping orbit
- files starting in orbit
- files ending in orbit
- precedessor to a file in-time on same instrument
- successor to a file in-time on same instrument

Others can be constructed using the primitives

## Notes for Writing Customized Queries

### Converting between human and epoch times:
The date command can be used to convert between human-style date strings and epoch integer times.    For example:
```
% date -d '2000-02-25 00:25:07' -u +%s
951438307
% date -d@951438307
Thu Feb 24 18:25:07 CST 2000
```

### Ordering Query Results:
The *ORDER BY* sql constructs can be used to order by instrument, time, filename, or various combinations.   For example, to order result per-instrument by start-time:
```
% sqlite3 accesslist.sqlite select fname from modis order by instrument, stime, asc
```


These constructs can be combined to construct powerful queries from the shell directly in scripts.    The queries can also be embedded in python, C, and many other languages if desired.

### Time-based queries:
* Find the orbit data for a date on the command line:
```
fusionBuildDB -o -q --discards=discards.txt --anomalies=errors.txt accesslist.sqlite data/Orbit_Path_Time.txt.gz data/MODIS.list.gz data/ASTER.list.gz data/MOPITT.list.gz data/MISR.list.gz data/CERES.list.gz
```
- One-orbit test Build
```
fusionBuildDB -o -v --discards=discards.txt --trace=trace.txt --anomalies=errors.txt oneorbit.sqlite data/Orbit_Path_Time.txt.gz testdata/one-orbit-files.txt.gz
```

