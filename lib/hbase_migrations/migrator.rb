module HbaseMigrations
  
  class Migrator#:nodoc:
      class << self

        def migrate(migrations_path, server, user, env,target_version = nil)
          hbase_connection(server).initialize_schema_information(user,env)
          case
            when target_version.nil?, current_version(server,user,env) < target_version
              up(migrations_path, server,user, env, target_version)
            when current_version(server,user,env) > target_version
              down(migrations_path,server, user, env, target_version)
            when current_version(server,user,env) == target_version
              return # You're on the right version
          end
        end

        def up(migrations_path,server,user, env,  target_version = nil)
          self.new(:up, migrations_path, server,user, env, target_version).migrate
        end

        def down(migrations_path, server,user, env, target_version = nil)
          self.new(:down, migrations_path, server,user, env, target_version).migrate
        end

        def current_version(server,user,env)   
          hbase_connection(server).current_schema_version(user,env).to_i
        end

        def hbase_connection(server)
          HbaseRecord::Base.establish_connection(server)
        end
        
      end

      def initialize(direction, migrations_path, server,user, env, target_version = nil)
        @hbase_connection = self.class.hbase_connection(server)
        @direction, @migrations_path, @target_version = direction, migrations_path, target_version
        @server,@user,@env = server, user, env
      end

      def current_version
        self.class.current_version(@server,@user,@env)
      end

      def migrate
        puts "start of migration"
        
        migration_classes.each do |migration_class|
          if reached_target_version?(migration_class.version)
            puts "reached target version"
            break
          end

          puts "check for relevance"
          next if irrelevant_migration?(migration_class.version)

          puts "migration..."
          migration_class.migrate(@direction)
          set_schema_version(migration_class.version)
        end
      end

      def pending_migrations
        migration_classes.select { |m| m.version > current_version }
      end

      private
        def migration_classes
          migrations = migration_files.inject([]) do |migrations, migration_file|
            load(migration_file)
            version, name = migration_version_and_name(migration_file)
            assert_unique_migration_version(migrations, version.to_i)
            migrations << migration_class(name, version.to_i)
          end

          sorted = migrations.sort_by { |m| m.version }
          down? ? sorted.reverse : sorted
        end

        def assert_unique_migration_version(migrations, version)
          if !migrations.empty? && migrations.find { |m| m.version == version }
            raise DuplicateMigrationVersionError.new(version)
          end
        end

        def migration_files
          puts "migrations_path: #{@migrations_path}"
          files = Dir["#{@migrations_path}/[0-9]*_*.rb"].sort_by do |f|
            m = migration_version_and_name(f)
            raise IllegalMigrationNameError.new(f) unless m
            m.first.to_i
          end
          down? ? files.reverse : files
        end

        def migration_class(migration_name, version)
          klass = migration_name.camelize.constantize
          class << klass; attr_accessor :version end
          klass.version = version
          klass.user = @user
          klass.env = @env
          klass.server = @server
          klass
        end

        def migration_version_and_name(migration_file)
          return *migration_file.scan(/([0-9]+)_([_a-z0-9]*).rb/).first
        end

        def set_schema_version(version)
          version = down? ? version - 1 : version
          @hbase_connection.update_schema_version(@user,@env,version)
        end

        def up?
          @direction == :up
        end

        def down?
          @direction == :down
        end

        def reached_target_version?(version)
          return false if @target_version == nil
          (up? && version.to_i - 1 >= @target_version) || (down? && version.to_i <= @target_version)
        end

        def irrelevant_migration?(version)
          (up? && version.to_i <= current_version) || (down? && version.to_i > current_version)
        end
    end
  
end
