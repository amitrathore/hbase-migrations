task :environment do
  require(File.join(APP_ROOT, 'config', 'boot'))
end


task :default => :spec

desc 'Runs all Specs'
task :spec do 
 system("spec .")
end