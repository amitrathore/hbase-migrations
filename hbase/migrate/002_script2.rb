class Script2 < HbaseMigrations::Migration
  
  def self.up
    create 't2', 'f1', 'f2', 'f3'
  end

  def self.down
    disable 't2'
    drop 't2'
  end
  
end
