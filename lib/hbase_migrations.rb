Dir["#{File.dirname(__FILE__)}/**/*.rb"].sort.each do |path|
  require "#{path}" 
end

# include Java
# include_class('java.lang.Integer') {|package,name| "J#{name}" }
# include_class('java.lang.Boolean') {|package,name| "J#{name}" }
# 
# import org.apache.hadoop.hbase.client.HBaseAdmin
# import org.apache.hadoop.hbase.client.HTable
# import org.apache.hadoop.hbase.HConstants
# import org.apache.hadoop.hbase.io.BatchUpdate
# import org.apache.hadoop.hbase.io.RowResult
# import org.apache.hadoop.hbase.io.Cell
# import org.apache.hadoop.hbase.HBaseConfiguration
# import org.apache.hadoop.hbase.HColumnDescriptor
# import org.apache.hadoop.hbase.HTableDescriptor
# import org.apache.hadoop.hbase.util.Bytes
# import org.apache.hadoop.hbase.util.Writables
# import org.apache.hadoop.hbase.HRegionInfo