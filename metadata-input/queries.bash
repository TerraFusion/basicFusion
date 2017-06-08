
#Example queries usable from bash scripts
#
#

#
# little support functions, see QUERIES at end
#

# run the query, $1 is DB, rest is query
DB() {
  local db=$1
  shift
  echo $* ";" | sqlite3 -separator / $db
}

selectFiles() {
  echo -n " select dir, fname from fileinfo inner join dirs on dirs.id == directory"
}

dbInfo() { # get the info table - build metadata
  sqlite3 $1 'select * from dbinfo;'
}

#For CERES and MOPITT only
overlapsOrbit() { # pass orbit arg
  echo  -n " where ((etime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 )) or (stime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 )) or (stime <(select stime from orbits where orbit = $1) and etime > (select etime from orbits where orbit = $1)))"
}

startsInOrbit() { # pass orbit arg
  echo  -n " where stime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 )"
}

endsInOrbit() { # pass orbit arg
  echo  -n " where etime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 )"
}

inOrbit(){
    echo  -n " where (stime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 )) and (etime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 ))"
}

forInstrument() #arg: MIS|AST|MOD|CER|...
{
  echo -n " and (select id from instruments where name = \"$1\" ) = instrument"
}


overlapExtraFiles()
{
    echo -n "UNION ALL select dir, fname from mis_add_fileinfo inner join dirs on dirs.id == directory inner join orbits on orbits.path == mis_add_fileinfo.path where (select stime from orbits where orbit = $1 ) <= orbits.etime and (select etime from orbits where orbit = $1 ) >= orbits.stime"
}

startExtraFiles()
{
    echo -n "UNION ALL select dir, fname from mis_add_fileinfo inner join dirs on dirs.id == directory inner join orbits on orbits.path == mis_add_fileinfo.path where (select stime from orbits where orbit = $1 ) == orbits.stime and (select etime from orbits where orbit = $1 ) <= orbits.etime"
}

endExtraFiles()
{
    echo -n "UNION ALL select dir, fname from mis_add_fileinfo inner join dirs on dirs.id == directory inner join orbits on orbits.path == mis_add_fileinfo.path where (select stime from orbits where orbit = $1 ) =< orbits.stime and (select etime from orbits where orbit = $1 ) == orbits.etime"
}

getExtraFiles()
{
    echo -n "UNION ALL select dir, fname from mis_add_fileinfo inner join dirs on dirs.id == directory inner join orbits on orbits.path == mis_add_fileinfo.path where orbits.orbit == $1"
}

# predecessor to file from same instrument
preceeds() {
  # arg:  $1=filename as returned from other queries
  local fn=$(basename $1)
  local dn=$(dirname $1)
  echo -n " where stime < (select stime from fileinfo where fname = \"$fn\")
  and fileinfo.instrument = (select instrument from fileinfo where fname = \"$fn\")
  and dir = \"$dn\"
  order by stime desc
  limit 1"
}

# next in-time file from same instrument
follows() {
  # arg:  $1=filename as returned from other queries
  local fn=$(basename $1)
  local dn=$(dirname $1)
  echo -n " where stime > (select stime from fileinfo where fname = \"$fn\")
  and fileinfo.instrument = (select instrument from fileinfo where fname = \"$fn\")
  and dir = \"$dn\"
  order by stime
  limit 1"
}

#
# QUERIES
#
#For CERES and MOPITT only
allOverlappingOrbit() { 
  # args: $1=database $2=orbit#
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles) $(overlapsOrbit $2) $(overlapExtraFiles $2)
}

#For CERES and MOPITT only
instrumentOverlappingOrbit(){
  # args:  $1=database $2=orbit# $3=AST|MOD|MIS|CER|MOP
  # example: instrumentOverlapsForOrbit fusionMetaDB.sqlite 4110 MIS 
  DB $1 $(selectFiles) $(overlapsOrbit $2) $(forInstrument $3) $(overlapExtraFiles $2) $(forInstrument $3)
}

allStartingInOrbit() { 
  # args: $1=database $2=orbit#
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles) $(startsInOrbit $2) $(startExtraFiles $2)
}

allEndingInOrbit() { 
  # args: $1=database $2=orbit#
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles) $(endsInOrbit $2) $(endExtraFiles $2)
}

allInOrbit(){
  # args: $1=database $2=orbit#
  # example: allInOrbit fusionMetaDB.sqlite 4110 
  DB $1 $(selectFiles) $(inOrbit $2) $(getExtraFiles $2)
}

#Exact orbit
instrumentInOrbit(){
  # args: $1=database $2=orbit#, $3=AST|MOD|MIS|CER|MOP
  # example: instrumentInOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles) $(inOrbit $2) $(forInstrument $3) $(getExtraFiles $2) $(forInstrument $3)
}

instrumentStartingInOrbit() { 
  # args: $1=database $2=orbit#, $3=AST|MOD|MIS|CER|MOP
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles) $(startsInOrbit $2) $(forInstrument $3) $(startExtraFiles $2) $(forInstrument $3)
}

preceedingFile() {
  # args: $1=database $2=filename as returned by other query
  DB $1 $(selectFiles) $(preceeds $2)
}

followingFile() {
  # args: $1=database $2=filename as returned by other query
  DB $1 $(selectFiles) $(follows $2)
}
