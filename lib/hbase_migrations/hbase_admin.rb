class HbaseAdmin
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
    @admin.tableExists(tableName).to_s
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

end
