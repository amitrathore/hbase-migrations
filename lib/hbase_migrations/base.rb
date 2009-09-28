require 'base64'
require 'yaml'
require 'set'
require 'erb'

module HbaseRecord #:nodoc:

  class Base

      def self.establish_connection(server)
        configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()
        
        options = YAML::load(ERB.new(IO.read(hbase_configuration_file)).result)
        
        server_options = options[server]
        
        configuration.setInt("hbase.client.retries.number", server_options['hbase_client_retries_number'])
        configuration.setInt("ipc.client.connect.max.retries", server_options['ipc_client_connect_max_retries'])
        configuration.set("hbase.master", server_options['hbase_master'])
        
        HbaseConnection.new(server,configuration)
      end
      
      private
      
      def self.hbase_configuration_file
        app_root = File.expand_path(File.join(File.dirname(__FILE__), '/../../')) 
        File.join(app_root, 'config', 'hbase.yml')
      end
        
  end

end
