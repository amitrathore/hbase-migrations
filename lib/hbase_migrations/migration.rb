module HbaseMigrations
  
  class IrreversibleMigration < StandardError#:nodoc:
  end

  class DuplicateMigrationVersionError < StandardError#:nodoc:
    def initialize(version)
      super("Multiple migrations have the version number #{version}")
    end
  end

  class IllegalMigrationNameError < StandardError#:nodoc:
    def initialize(name)
      super("Illegal name for migration file: #{name}\n\t(only lower case letters, numbers, and '_' allowed)")
    end
  end


  class Migration
    extend HbaseCommands
    
    @@verbose = true

    class << self
      attr_accessor :verbose
       
      def up
        migrate(:up)
      end

      def down
        migrate(:down)
      end

      # Execute this migration in the named direction
      def migrate(direction)
        return unless respond_to?(direction)

        case direction
          when :up   then announce "migrating"
          when :down then announce "reverting"
        end

        result = send(direction)

        case direction
          when :up   then announce "migrated "; write
          when :down then announce "reverted "; write
        end

        result
      end

      def write(text="")
        puts(text)
      end

      def announce(message)
        text = "#{@version} #{name}: #{message}"
        length = [0, 75 - text.length].max
        write "== %s %s" % [text, "=" * length]
      end

      def say(message, subitem=false)
        write "#{subitem ? "   ->" : "--"} #{message}"
      end

    end
    
  end

  class Migrator#:nodoc:
    class << self
      
      def migrate(migrations_path, target_version = nil)
        hbase_connection.initialize_schema_information
        case
          when target_version.nil?, current_version < target_version
            up(migrations_path, target_version)
          when current_version > target_version
            down(migrations_path, target_version)
          when current_version == target_version
            return # You're on the right version
        end
      end

      def up(migrations_path, target_version = nil)
        self.new(:up, migrations_path, target_version).migrate
      end

      def down(migrations_path, target_version = nil)
        self.new(:down, migrations_path, target_version).migrate
      end

      def schema_info_table_name
        Base.table_name_prefix + "schema_info" + Base.table_name_suffix
      end

      def current_version   
        hbase_connection.current_schema_version('siva','test').to_i
      end

      def proper_table_name(name)
        # Use the ActiveRecord objects own table_name, or pre/suffix from ActiveRecord::Base if name is a symbol/string
        name.table_name rescue "#{ActiveRecord::Base.table_name_prefix}#{name}#{ActiveRecord::Base.table_name_suffix}"
      end
      
      def hbase_connection
        HbaseConnection.new
      end
    end

    def initialize(direction, migrations_path, target_version = nil)
      @hbase_connection = self.class.hbase_connection
      @direction, @migrations_path, @target_version = direction, migrations_path, target_version
      @hbase_connection.initialize_schema_information
    end

    def current_version
      self.class.current_version
    end

    def migrate
      migration_classes.each do |migration_class|
       if reached_target_version?(migration_class.version)
          break
        end
        
        next if irrelevant_migration?(migration_class.version)

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
        klass
      end

      def migration_version_and_name(migration_file)
        return *migration_file.scan(/([0-9]+)_([_a-z0-9]*).rb/).first
      end

      def set_schema_version(version)
        version = down? ? version - 1 : version
        @hbase_connection.update_schema_version('siva','test',version)
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
