namespace :hbase do

  desc "Migrate the hbase through scripts in hb/migrate. Target specific version with VERSION=x. "
  task :migrate => :check do
    HbaseMigrations::Migration.verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] == "true" : true
    HbaseMigrations::Migrator.migrate("hb/migrate/", ENV["USER"], ENV["ENV"], ENV["VERSION"] ? ENV["VERSION"].to_i : nil)
  end

  desc 'Rolls the schema back to the previous version. Specify the number of steps with STEP=n'
  task :rollback => :check do
    step = ENV['STEP'] ? ENV['STEP'].to_i : 1
    version = HbaseMigrations::Migrator.current_version(ENV["USER"], ENV["ENV"]) - step
    HbaseMigrations::Migrator.migrate('hb/migrate/', ENV["USER"], ENV["ENV"] , version)
  end

  desc "Retrieves the current schema version number"
  task :version => :check do
    puts "Current version: #{HbaseMigrations::Migrator.current_version  ENV["USER"], ENV["ENV"]}"
  end

  desc "Raises an error if there are pending migrations"
  task :abort_if_pending_migrations => :check do
    pending_migrations = HbaseMigrations::Migrator.new(:up, ENV["USER"], ENV["ENV"] ,'hb/migrate').pending_migrations
    
    if pending_migrations.any?
     puts "You have #{pending_migrations.size} pending migrations:"
     pending_migrations.each do |pending_migration|
       puts '  %4d %s' % [pending_migration.version, pending_migration.name]
     end
     abort "Run `rake hbase:migrate` to update your database then try again."
    end
  end
  
  # "Checks whether username and environment is passed or not"
  task :check => :environment do
    raise "Need to pass USER and ENV variables" if ENV['USER'].nil? or ENV['ENV'].nil?
  end

end
