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
      attr_accessor :user
      attr_accessor :env
       
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

end
