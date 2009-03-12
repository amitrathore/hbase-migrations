#require 'hbase_table'

class HbaseConnection
  
  def initialize
    @configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()
    @configuration.setInt("hbase.client.retries.number", 5)
    @configuration.setInt("ipc.client.connect.max.retries", 3) 
  end
  
  def current_schema_version(user,env)
    table = HbaseTable.new(@configuration,'schema_versions')
    answer = table.get("#{user}:#{env}")
    answer["siva:test"]["version:"]
  end
  
  def update_schema_version(user,env,new_version)
    table = HbaseTable.new(@configuration,'schema_versions')
    table.put("#{user}:#{env}","version:",new_version.to_s)
  end
  
   def initialize_schema_information
   #     begin
   #       execute "CREATE TABLE #{quote_table_name(ActiveRecord::Migrator.schema_info_table_name)} (version #{type_to_sql(:integer)})"
   #       execute "INSERT INTO #{quote_table_name(ActiveRecord::Migrator.schema_info_table_name)} (version) VALUES(0)"
   #     rescue ActiveRecord::StatementInvalid
   #       # Schema has been initialized
   #     end
   end
    
end


