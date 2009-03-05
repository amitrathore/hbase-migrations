Dir["#{APP_ROOT}/lib/**/*.rb"].sort.each do |path|
  require "#{path}" 
end
