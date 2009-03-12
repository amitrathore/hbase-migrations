class Script1 < HbaseMigrations::Migration
  
  def self.up
    create 't1', 'f1', 'f2', 'f3'
  end

  def self.down
    disable 't1'
    drop 't1'
  end
  
end
