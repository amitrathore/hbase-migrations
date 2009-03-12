#require 'hbase_table'

class HbaseConnection
  attr_accessor :configuration
  
  def initialize
    @configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()
    @configuration.setInt("hbase.client.retries.number", 5)
    @configuration.setInt("ipc.client.connect.max.retries", 3) 
  end
  
  def current_schema_version(user,env)
    table = HbaseTable.new(@configuration,'schema_versions')
    answer = table.get("#{user}:#{env}")
    answer["#{user}:#{env}"]["version:"] 
  end
  
  def update_schema_version(user,env,new_version)
    table = HbaseTable.new(@configuration,'schema_versions')
    table.put("#{user}:#{env}","version:",new_version.to_s)
  end
  
   def initialize_schema_information(user,env)
     admin = HbaseAdmin.new     
     admin.create('schema_versions','version') unless admin.exists('schema_versions') == 'true'
     
     table = HbaseTable.new(@configuration,'schema_versions')
     table.put("#{user}:#{env}", "version:", '0') if  table.get("#{user}:#{env}").empty?

     admin.flush('schema_versions')
   end
    
end


