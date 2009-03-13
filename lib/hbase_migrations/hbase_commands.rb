=begin
HBASE SURGERY TOOLS:
 close_region    Close a single region. Optionally specify regionserver.
                 Examples:
                 
                 hbase> close_region 'REGIONNAME'
                 hbase> close_region 'REGIONNAME', 'REGIONSERVER_IP:PORT'

 compact         Compact all regions in passed table or pass a region row
                 to compact an individual region

 disable_region  Disable a single region

 enable_region   Enable a single region. For example:
  
                 hbase> enable_region 'REGIONNAME'
 
 flush           Flush all regions in passed table or pass a region row to
                 flush an individual region.  For example:

                 hbase> flush 'TABLENAME'
                 hbase> flush 'REGIONNAME'

 major_compact   Run major compaction on passed table or pass a region row
                 to major compact an individual region

 split           Split table or pass a region row to split individual region

Above commands are for 'experts'-only as misuse can damage an install

=end

=begin
HBASE SHELL COMMANDS:
 alter     Alter column family schema;  pass table name and a dictionary
           specifying new column family schema. Dictionaries are described
           below in the GENERAL NOTES section.  Dictionary must include name
           of column family to alter.  For example, 
           
           To change or add the 'f1' column family in table 't1' from defaults
           to instead keep a maximum of 5 cell VERSIONS, do:
           hbase> alter 't1', {NAME => 'f1', VERSIONS => 5}
           
           To delete the 'f1' column family in table 't1', do:
           hbase> alter 't1', {NAME => 'f1', METHOD => 'delete'}
           
 count     Count the number of rows in a table. This operation may take a LONG
           time (Run '$HADOOP_HOME/bin/hadoop jar hbase.jar rowcount' to run a
           counting mapreduce job). Current count is shown every 1000 rows by
           default. Count interval may be optionally specified. Examples:
           
           hbase> count 't1'
           hbase> count 't1', 100000

 create    Create table; pass table name, a dictionary of specifications per
           column family, and optionally a dictionary of table configuration.
           Dictionaries are described below in the GENERAL NOTES section.
           Examples:

           hbase> create 't1', {NAME => 'f1', VERSIONS => 5}
           hbase> create 't1', {NAME => 'f1'}, {NAME => 'f2'}, {NAME => 'f3'}
           hbase> # The above in shorthand would be the following:
           hbase> create 't1', 'f1', 'f2', 'f3'
           hbase> create 't1', {NAME => 'f1', VERSIONS => 1, TTL => 2592000, \\
             BLOCKCACHE => true}

 describe  Describe the named table: e.g. "hbase> describe 't1'"

 delete    Put a delete cell value at specified table/row/column and optionally
           timestamp coordinates.  Deletes must match the deleted cell's
           coordinates exactly.  When scanning, a delete cell suppresses older
           versions. Takes arguments like the 'put' command described below
 
 deleteall Delete all cells in a given row; pass a table name, row, and optionally 
           a column and timestamp

 disable   Disable the named table: e.g. "hbase> disable 't1'"
 
 drop      Drop the named table. Table must first be disabled

 enable    Enable the named table

 exists    Does the named table exist? e.g. "hbase> exists 't1'"

 exit      Type "hbase> exit" to leave the HBase Shell

 get       Get row or cell contents; pass table name, row, and optionally
           a dictionary of column(s), timestamp and versions.  Examples:

           hbase> get 't1', 'r1'
           hbase> get 't1', 'r1', {COLUMN => 'c1'}
           hbase> get 't1', 'r1', {COLUMN => ['c1', 'c2', 'c3']}
           hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
           hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, \\
             VERSIONS => 4}

 list      List all tables in hbase

 put       Put a cell 'value' at specified table/row/column and optionally
           timestamp coordinates.  To put a cell value into table 't1' at
           row 'r1' under column 'c1' marked with the time 'ts1', do:

           hbase> put 't1', 'r1', 'c1', 'value', ts1

 tools     Listing of hbase surgery tools

 scan      Scan a table; pass table name and optionally a dictionary of scanner 
           specifications.  Scanner specifications may include one or more of 
           the following: LIMIT, STARTROW, STOPROW, TIMESTAMP, or COLUMNS.  If 
           no columns are specified, all columns will be scanned.  To scan all 
           members of a column family, leave the qualifier empty as in 
           'col_family:'.  Examples:
           
           hbase> scan '.META.'
           hbase> scan '.META.', {COLUMNS => 'info:regioninfo'}
           hbase> scan 't1', {COLUMNS => ['c1', 'c2'], LIMIT => 10, \\
             STARTROW => 'xyz'}

 truncate  Disables, drops and recreates the specified table.
           
 version   Output this HBase version

GENERAL NOTES:
Quote all names in the hbase shell such as table and column names.  Don't
forget commas delimit command parameters.  Type <RETURN> after entering a
command to run it.  Dictionaries of configuration used in the creation and
alteration of tables are ruby Hashes. They look like this:

  {'key1' => 'value1', 'key2' => 'value2', ...}

They are opened and closed with curley-braces.  Key/values are delimited by
the '=>' character combination.  Usually keys are predefined constants such as
NAME, VERSIONS, COMPRESSION, etc.  Constants do not need to be quoted.  Type
'Object.constants' to see a (messy) list of all constants in the environment.

This HBase shell is the JRuby IRB with the above HBase-specific commands added.
For more on the HBase Shell, see http://wiki.apache.org/hadoop/Hbase/Shell
=end

module HbaseCommands
  # DDL

  def admin()
    @admin = HbaseAdmin.new(server) unless @admin
    @admin
  end

  def table(table)
    HbaseTable.new(@configuration, table_name(table))
  end

  def create(table, *args)
    admin().create(table_name(table), args)
  end

  def drop(table)
    admin().drop(table_name(table))
  end

  def alter(table, args)
    admin().alter(table_name(table), args) 
  end

  # Administration

  def list
    admin().list()
  end

  def describe(table)
    admin().describe(table_name(table))
  end
  
  def enable(table)
    admin().enable(table_name(table))
  end

  def disable(table)
    admin().disable(table_name(table))
  end

  def enable_region(regionName)
    admin().enable_region(regionName)
  end

  def disable_region(regionName)
    admin().disable_region(regionName)
  end

  def exists(table)
    admin().exists(table_name(table))
  end

  def truncate(table)
    admin().truncate(table_name(table))
  end

  def close_region(regionName, server = nil)
    admin().close_region(regionName, server)
  end
  
  # CRUD
  
  def get(table, row, args = {})
    table(table_name(table)).get(row, args)
  end

  def put(table, row, column, value, timestamp = nil)
    table(table_name(table)).put(row, column, value, timestamp)
  end
  
  def scan(table, args = {})
    table(table_name(table)).scan(args)
  end
  
  def delete(table, row, column,
      timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
    table(table_name(table)).delete(row, column, timestamp)
  end

  def deleteall(table, row, column = nil,
    timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
    table(table_name(table)).deleteall(row, column, timestamp)
  end

  def count(table, interval = 1000)
    table(table_name(table)).count(interval)
  end

  def flush(tableNameOrRegionName)
    admin().flush(table_name(tableNameOrRegionName))
  end

  def compact(tableNameOrRegionName)
    admin().compact(table_name(tableNameOrRegionName))
  end

  def major_compact(tableNameOrRegionName)
    admin().major_compact(table_name(tableNameOrRegionName))
  end

  def split(tableNameOrRegionName)
    admin().split(table_name(tableNameOrRegionName))
  end
  
  def table_name(table)
    "#{user}_#{env}_#{table}"
  end

end
