require 'base64'
require 'yaml'
require 'set'

module HbaseRecord #:nodoc:

  class Base

      # Establishes the connection to the HBase. Accepts a hash as input
      #
      #   HbaseRecord::Base.establish_connection(
      #     :host     => "localhost",
      #     :username => "myuser",
      #     :password => "mypass",
      #     :database => "somedatabase"
      #   )
      #
  
      def self.establish_connection(spec = nil)
        configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()
        configuration.setInt("hbase.client.retries.number", 5)
        configuration.setInt("ipc.client.connect.max.retries", 3)
        
        HbaseConnection.new(configuration)
      end
  
  end

end
