require 'spec'

Dir["#{File.dirname(__FILE__)}/../lib/**/*.rb"].sort.each do |path|
  require "#{path}" 
end

Dir["#{File.dirname(__FILE__)}/../spec/data/*.rb"].sort.each do |path|
  require "#{path}" 
end