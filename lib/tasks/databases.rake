namespace :hbase do

  desc "Migrate the database through scripts in db/migrate. Target specific version with VERSION=x. Turn off output with VERBOSE=false."
  task :migrate => :environment do
    HbaseMigrations::Migration.verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] == "true" : true
    HbaseMigrations::Migrator.migrate("hb/migrate/", ENV["VERSION"] ? ENV["VERSION"].to_i : nil)
  end

  desc 'Rolls the schema back to the previous version. Specify the number of steps with STEP=n'
  task :rollback => :environment do
    step = ENV['STEP'] ? ENV['STEP'].to_i : 1
    version = HbaseMigrations::Migrator.current_version - step
    HbaseMigrations::Migrator.migrate('hb/migrate/', version)
  end

  desc "Retrieves the current schema version number"
  task :version => :environment do
    puts "Current version: #{HbaseMigrations::Migrator.current_version}"
  end

  desc "Raises an error if there are pending migrations"
  task :abort_if_pending_migrations => :environment do
    pending_migrations = HbaseMigrations::Migrator.new(:up, 'hb/migrate').pending_migrations
    
    if pending_migrations.any?
     puts "You have #{pending_migrations.size} pending migrations:"
     pending_migrations.each do |pending_migration|
       puts '  %4d %s' % [pending_migration.version, pending_migration.name]
     end
     abort "Run `rake hbase:migrate` to update your database then try again."
    end
  end

end
