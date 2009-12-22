module HbaseCommandConstants
  COLUMN = "COLUMN"
  COLUMNS = "COLUMNS"
  TIMESTAMP = "TIMESTAMP"
  NAME = org.apache.hadoop.hbase.HConstants::NAME
  VERSIONS = org.apache.hadoop.hbase.HConstants::VERSIONS
  IN_MEMORY = org.apache.hadoop.hbase.HConstants::IN_MEMORY
  STOPROW = "STOPROW"
  STARTROW = "STARTROW"
  ENDROW = STOPROW
  LIMIT = "LIMIT"
  METHOD = "METHOD"
  MAXLENGTH = "MAXLENGTH"
  CACHE_BLOCKS = "CACHE_BLOCKS"

  include_class Java::org.apache.hadoop.hbase.HColumnDescriptor
end

class HbaseAdmin
  include HbaseCommandConstants
  
  attr_reader :configuration
  
  def initialize(server)
    hbase_connection = HbaseRecord::Base.establish_connection(server)
    @configuration = hbase_connection.configuration
    @admin = Java::OrgApacheHadoopHbaseClient::HBaseAdmin.new(@configuration)
  end

  def all_tables
    @admin.listTables.map { |table| table.getNameAsString }
  end
  
  def exists(tableName)
    @admin.tableExists(tableName)
  end

  def flush(tableNameOrRegionName)
    @admin.flush(tableNameOrRegionName)
  end

  def compact(tableNameOrRegionName)
    @admin.compact(tableNameOrRegionName)
  end

  def major_compact(tableNameOrRegionName)
    @admin.majorCompact(tableNameOrRegionName)
  end

  def split(tableNameOrRegionName)
    @admin.split(tableNameOrRegionName)
  end

  def enable(tableName)
    @admin.enableTable(tableName)
  end

  def disable(tableName)
    @admin.disableTable(tableName)
  end

  def drop(tableName)
    if @admin.isTableEnabled(tableName)
      raise IOError.new("Table " + tableName + " is enabled. Disable it first")
    else
      @admin.deleteTable(tableName)
    end
  end

  def truncate(tableName)
    now = Time.now
    hbaseTable = HbaseTable.new(@configuration, tableName)
    tableDescription = hbaseTable.table_descriptor
    puts 'Truncating ' + tableName + '; it may take a while'
    puts 'Disabling table...'
    disable(tableName)
    puts 'Dropping table...'
    drop(tableName)
    puts 'Creating table...'
    @admin.createTable(tableDescription)
  end

  # Pass tablename and an array of Hashes
  def create(tableName, args)
    now = Time.now 
    # Pass table name and an array of Hashes.  Later, test the last
    # array to see if its table options rather than column family spec.
    raise TypeError.new("Table name must be of type String") \
      unless tableName.instance_of? String
    # For now presume all the rest of the args are column family
    # hash specifications. TODO: Add table options handling.
    htd = Java::OrgApacheHadoopHbase::HTableDescriptor.new(tableName)
    for arg in args
      if arg.instance_of? String
        htd.addFamily(Java::OrgApacheHadoopHbase::HColumnDescriptor.new(makeColumnName(arg)))
      else
        raise TypeError.new(arg.class.to_s + " of " + arg.to_s + " is not of Hash type") \
          unless arg.instance_of? Hash
        htd.addFamily(hcd(arg))
      end
    end
    @admin.createTable(htd)
  end

  def alter(tableName, args)
    now = Time.now
    raise TypeError.new("Table name must be of type String") \
      unless tableName.instance_of? String
    htd = @admin.getTableDescriptor(tableName.to_java_bytes)
    method = args.delete(METHOD)
    if method == "delete"
      @admin.deleteColumn(tableName, makeColumnName(args[NAME]))
    else
      descriptor = hcd(args) 
      if (htd.hasFamily(descriptor.getNameAsString().to_java_bytes))
        @admin.modifyColumn(tableName, descriptor.getNameAsString(), 
                            descriptor);
      else
        @admin.addColumn(tableName, descriptor);
      end
    end
  end

  def close_region(regionName, server)
    now = Time.now
    s = nil
    s = [server].to_java if server
    @admin.closeRegion(regionName, s)
  end

  # Make a legal column  name of the passed String
  # Check string ends in colon. If not, add it.
  def makeColumnName(arg)
    index = arg.index(':')
    if not index
      # Add a colon.  If already a colon, its in the right place,
      # or an exception will come up out of the addFamily
      arg << ':'
    end
    arg
  end

  def hcd(arg)
    # Return a new HColumnDescriptor made of passed args
    # TODO: This is brittle code.
    # Here is current HCD constructor:
    # public HColumnDescriptor(final byte [] familyName, final int maxVersions,
    # final String compression, final boolean inMemory,
    # final boolean blockCacheEnabled, final int blocksize,
    # final int maxValueLength,
    # final int timeToLive, final boolean bloomFilter) {
    name = arg[NAME]
    raise ArgumentError.new("Column family " + arg + " must have a name") unless name
    name = makeColumnName(name)
    # TODO: What encoding are Strings in jruby?
    return HColumnDescriptor.new(name.to_java_bytes,
                                 # JRuby uses longs for ints. Need to convert.  Also constants are String 
                                 arg[VERSIONS]? JInteger.new(arg[VERSIONS]): HColumnDescriptor::DEFAULT_VERSIONS,
                                 arg[HColumnDescriptor::COMPRESSION]? arg[HColumnDescriptor::COMPRESSION]: HColumnDescriptor::DEFAULT_COMPRESSION,
                                 arg[IN_MEMORY]? JBoolean.valueOf(arg[IN_MEMORY]): HColumnDescriptor::DEFAULT_IN_MEMORY,
                                 arg[HColumnDescriptor::BLOCKCACHE]? JBoolean.valueOf(arg[HColumnDescriptor::BLOCKCACHE]): HColumnDescriptor::DEFAULT_BLOCKCACHE,
                                 arg[HColumnDescriptor::BLOCKSIZE]? JInteger.valueOf(arg[HColumnDescriptor::BLOCKSIZE]): HColumnDescriptor::DEFAULT_BLOCKSIZE,
                                 arg[HColumnDescriptor::TTL]? JInteger.new(arg[HColumnDescriptor::TTL]): HColumnDescriptor::DEFAULT_TTL,
                                 arg[HColumnDescriptor::BLOOMFILTER]? JBoolean.valueOf(arg[HColumnDescriptor::BLOOMFILTER]): HColumnDescriptor::DEFAULT_BLOOMFILTER)
  end

end
