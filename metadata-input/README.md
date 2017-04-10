
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
CER_BDS_Terra-FM2_Edition4_400403.20070702
                                  YYYYMMDD
                                  ^^^^^^^^
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

## Query Examples
With the start and end time of all files in the DB many useful queries can be constructed in sql.   The BETWEEN operator can find times that lie within a particular span, and overlapping timespans can be selected on 2 ranges with start1/end1 and start2/end2 by selecting on *start1 <= end2 and end1 >= start2*

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
% sqlite3 accesslist.sqlite 'select orbit from orbits where '"`date -d'2000-02-25 00:25:07' -u +%s`"' between stime and etime'
1000

% sqlite3 accesslist.sqlite 'select stime,  etime from orbits where '"`date -d'2000-02-25 00:25:07' -u +%s`"' between stime and etime' | tr \| ' '
951438307 951444240

% sqlite3 accesslist.sqlite 'select path from orbits where '"`date -d'2000-02-25 00:25:07' -u +%s`"' between stime and etime'
100
```
* Find all the files that overlap some time span:
```
% sqlite3 oneorbit.sqlite 'select dir, fname from modis inner join dirs on dirs.id == directory where '"`date -d'Tue Jul  3 11:09:15 CDT 2007' -u +%s`"' <= etime and '"`date -d'Tue Jul  3 12:48:08 CDT 2007' -u +%s`"' >= stime' | tr \| /
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD021KM.A2007184.1610.006.2014231113627.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD03.A2007184.1610.006.2012239144154.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD021KM.A2007184.1615.006.2014231113638.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD03.A2007184.1615.006.2012239144035.hdf
 ... (many lines)
``` 

### Orbit-oriented queries:
* Find all files that overlap orbit 40110:
```
% sqlite3 oneorbit.sqlite 'select dir, fname from modis inner join dirs on dirs.id == directory where (select stime from orbits where orbit = '40110' ) <= etime and (select etime from orbits where orbit = '40110' ) >= stime' | tr \| /
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD021KM.A2007184.1610.006.2014231113627.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD03.A2007184.1610.006.2012239144154.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD021KM.A2007184.1615.006.2014231113638.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD03.A2007184.1615.006.2012239144035.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD021KM.A2007184.1620.006.2014231113646.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD03.A2007184.1620.006.2012239144326.hdf
 ...
```
* Find the files that overlap the start of some orbit:
```
% sqlite3 oneorbit.sqlite 'select dir, fname from modis inner join dirs on dirs.id == directory where (select stime from orbits where orbit = '40110' ) between stime and etime' | tr \| /
/projects/sciteam/jq0/TerraFusion/oneorbit/CERES/CER_BDS_Terra-FM1_Edition3_032040.20070703
/projects/sciteam/jq0/TerraFusion/oneorbit/MOPITT/MOP01-20070703-L1V3.50.0.he5
/projects/sciteam/jq0/TerraFusion/oneorbit/MOPITT/MOP01-20070703-L1V3.50.0.he5.xml
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AF_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AN_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_BA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_BF_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_CA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_CF_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_DA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_DF_F03_0024.hdf

```
* Find the files that overlap the *end* of some orbit:
```
% sqlite3 oneorbit.sqlite 'select dir, fname from modis inner join dirs on dirs.id == directory where (select etime from orbits where orbit = '40110' ) between stime and etime' | tr \| /
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD021KM.A2007184.1745.006.2014231114122.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MODIS/MOD03.A2007184.1745.006.2012239144155.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/CERES/CER_BDS_Terra-FM1_Edition3_032040.20070703
/projects/sciteam/jq0/TerraFusion/oneorbit/MOPITT/MOP01-20070703-L1V3.50.0.he5
/projects/sciteam/jq0/TerraFusion/oneorbit/MOPITT/MOP01-20070703-L1V3.50.0.he5.xml
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AF_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AN_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_BA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_BF_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_CA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_CF_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_DA_F03_0024.hdf
/projects/sciteam/jq0/TerraFusion/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_DF_F03_0024.hdf

```

