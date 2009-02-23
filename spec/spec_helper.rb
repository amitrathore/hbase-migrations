require 'spec'

Dir["#{File.dirname(__FILE__)}/../lib/**"].map do |file|
  load file unless File.directory?(file)
end