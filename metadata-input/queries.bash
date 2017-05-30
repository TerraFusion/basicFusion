

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
  echo -n "with files as (select dir, fname, fsize from fileinfo inner join dirs on dirs.id == directory" $* ")" \
    "select dir, fname from files union select 'total bytes', sum(fsize) from files"
}

dbInfo() { # get the info table - build metadata
  sqlite3 $1 'select * from dbinfo;'
}

overlapsOrbit() { # pass orbit arg
  echo  -n " where (select stime from orbits where orbit = $1 ) <= etime 
  and (select etime from orbits where orbit = $1 ) >= stime"
}

startsInOrbit() { # pass orbit arg
  echo  -n " where stime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 )"
}

endsInOrbit() { # pass orbit arg
  echo  -n " where etime between (select stime from orbits where orbit = $1) and (select etime from orbits where orbit = $1 )"
}

forInstrument() #arg: MIS|AST|MOD|CER|...
{
  echo -n " and (select id from instruments where name = \"$1\" ) = instrument"
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
allOverlappingOrbit() { 
  # args: $1=database $2=orbit#
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles $(overlapsOrbit $2))
}

instrumentOverlappingOrbit(){
  # args:  $1=database $2=orbit# $3=AST|MOD|MIS|CER|MOP
  # example: instrumentOverlapsForOrbit fusionMetaDB.sqlite 4110 MIS 
  DB $1 $(selectFiles $(overlapsOrbit $2) $(forInstrument $3))
}

allStartingInOrbit() { 
  # args: $1=database $2=orbit#
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles $(startsInOrbit $2))
}

allEndingInOrbit() { 
  # args: $1=database $2=orbit#
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles $(endsInOrbit $2))
}

instrumentStartingInOrbit() { 
  # args: $1=database $2=orbit#, $3=AST|MOD|MIS|CER|MOP
  # example: allOverlapsForOrbit fusionMetaDB.sqlite 4110
  DB $1 $(selectFiles $(startsInOrbit $2) $(forInstrument $3))
}

preceedingFile() {
  # args: $1=database $2=filename as returned by other query
  DB $1 $(selectFiles $(preceeds $2))
}

followingFile() {
  # args: $1=database $2=filename as returned by other query
  DB $1 $(selectFiles $(follows $2))
}