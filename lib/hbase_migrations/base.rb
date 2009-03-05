require 'base64'
require 'yaml'
require 'set'

module HbaseRecord #:nodoc:
  
  class HbaseRecordError < StandardError
  end

  class ConnectionNotEstablished < HbaseRecordError
  end
  
  class StatementInvalid < HbaseRecordError
  end

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
      end
  
  end

end
