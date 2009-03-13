require File.dirname(__FILE__) + '/spec_helper'

describe HbaseMigrations::Migrator, " when migrate is called " do
    
    before(:each) do
        HbaseMigrations::Migrator.stub!(:current_version).and_return(1)
        stub_hbase_connection = mock('hbase_connection')
        stub_hbase_connection.stub!(:initialize_schema_information)
        HbaseRecord::Base.stub!(:establish_connection).and_return(stub_hbase_connection)
    end
    
    it "should tell hbase connection to initialize schema information" do      
      mock_hbase_connection = mock('hbase_connection')
      mock_hbase_connection.should_receive(:initialize_schema_information).with('user','env')
      HbaseRecord::Base.should_receive(:establish_connection).with('server').at_least(1).times.and_return(mock_hbase_connection)
     
      HbaseMigrations::Migrator.migrate('migrations_path', 'server', 'user', 'env',2)
    end
    
    describe " and target version is less than current verion" do
        describe " or target version is null" do
          
          before(:each) do
              @mock_migrator = mock('migrator')
              @mock_migrator.should_receive(:migrate)
          end
          
          it "should create Migrator object with direction :up" do
            HbaseMigrations::Migrator.should_receive(:new).
                                      with(:up,'migrations_path', 'server', 'user', 'env',2).
                                      and_return(@mock_migrator)
            
            HbaseMigrations::Migrator.migrate('migrations_path', 'server', 'user', 'env',2)
          end
          
          it "should call migrate with direction :up" do

            HbaseMigrations::Migrator.stub!(:new).and_return(@mock_migrator)
            
            HbaseMigrations::Migrator.migrate('migrations_path', 'server', 'user', 'env',2)
          end
          
        end
    end
    
    describe " and target version is greater than current version" do
        before(:each) do
             @mock_migrator = mock('migrator')
             @mock_migrator.should_receive(:migrate)
        end
         
        it "should create Migrator object with correct parameter with direction :down" do
            HbaseMigrations::Migrator.should_receive(:new).
                                      with(:down,'migrations_path', 'server', 'user', 'env',0).
                                      and_return(@mock_migrator)
            
            HbaseMigrations::Migrator.migrate('migrations_path', 'server', 'user', 'env',0)
        end
        
        it "should call migrate with direction :down" do
            
            HbaseMigrations::Migrator.stub!(:new).and_return(@mock_migrator)
            
            HbaseMigrations::Migrator.migrate('migrations_path', 'server', 'user', 'env',0)
        end
    end
    
    describe " and target version is equal to current version" do
      it "should not create Migrator object" do
          HbaseMigrations::Migrator.should_not_receive(:new)
          
          HbaseMigrations::Migrator.migrate('migrations_path', 'server', 'user', 'env',1)
      end
    end
  
end