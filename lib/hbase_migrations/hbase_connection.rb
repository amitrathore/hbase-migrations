#require 'hbase_table'

class HbaseConnection
  attr_accessor :configuration, :server
  
  def initialize(server,confirguration)
    @configuration = confirguration
    @server = server
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
     admin = HbaseAdmin.new(@server)    
     admin.create('schema_versions','version') unless admin.exists('schema_versions') == 'true'
     
     table = HbaseTable.new(@configuration,'schema_versions')
     table.put("#{user}:#{env}", "version:", '0') if  table.get("#{user}:#{env}").empty?
   end
    
end


