require File.dirname(__FILE__) + '/spec_helper'

describe HbaseMigrations::Migrator, " when migrate is called " do
    
    before(:each) do
        HbaseMigrations::Migrator.stub!(:current_version).and_return(1)
        @mock_hbase_connection = mock('hbase_connection')
        @mock_hbase_connection.stub!(:initialize_schema_information)
        @mock_hbase_connection.stub!(:update_schema_version)
        HbaseRecord::Base.stub!(:establish_connection).and_return(@mock_hbase_connection)
    end
    
    it "should set all values properly to migration classes" do
       TestScript2.should_receive(:user=).with('user')
       TestScript2.should_receive(:server=).with('server')
       TestScript2.should_receive(:env=).with('env')
       
       migrator = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',2)
       migrator.migrate
    end
    
    describe " and current version is 1" do
        describe " and direction is :up" do
          it "should call :up method in test script 2" do
            TestScript2.should_receive(:send).with(:up)
            
            migrator = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',2)
            migrator.migrate
          end
      
          it "should update schema to 2" do
            @mock_hbase_connection.should_receive(:update_schema_version).with('user', 'env',2)
            
            migrator = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',2)
            migrator.migrate
          end
        end
        
        describe " and direction is :down" do
          it "should call :down method in test script 1" do
              TestScript1.should_receive(:send).with(:down)

              migrator = HbaseMigrations::Migrator.new(:down,'spec/data', 'server', 'user', 'env',0)
              migrator.migrate
          end
      
          it "should update schema to 0" do
              @mock_hbase_connection.should_receive(:update_schema_version).with('user', 'env',0)

              migrator = HbaseMigrations::Migrator.new(:down,'spec/data', 'server', 'user', 'env',0)
              migrator.migrate
          end
        end
        
    end
    
    describe " and current version 0" do
      before(:each) do
          HbaseMigrations::Migrator.stub!(:current_version).and_return(0)
      end
      
      describe " and direction is :up" do
        describe "and target version is nil" do
            it "should call :up method in test script 2 and test script 1" do
              TestScript1.should_receive(:send).with(:up)
              TestScript2.should_receive(:send).with(:up)
          
              migrator = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env')
              migrator.migrate
            end
    
            it "should update schema to 2" do
              @mock_hbase_connection.should_receive(:update_schema_version).with('user', 'env',2)
          
              migrator = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env')
              migrator.migrate
            end
        end
        
        describe "and target version is 1" do
            it "should call :up method only in test script 1 and not in test script 2" do
              TestScript1.should_receive(:send).with(:up)
              TestScript2.should_not_receive(:send).with(:up)
          
              migrator = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',1)
              migrator.migrate
            end
    
            it "should update schema to 1" do
              @mock_hbase_connection.should_receive(:update_schema_version).with('user', 'env',1)
          
              migrator = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',1)
              migrator.migrate
            end
        end
      end
    end
    
    describe " and current version 2" do
      before(:each) do
          HbaseMigrations::Migrator.stub!(:current_version).and_return(2)
      end
      
      describe " and direction is :down" do
        describe "and target version is 0" do
            it "should call :down method in test script 2 and test script 1" do
              TestScript1.should_receive(:send).with(:down)
              TestScript2.should_receive(:send).with(:down)
          
              migrator = HbaseMigrations::Migrator.new(:down,'spec/data', 'server', 'user', 'env',0)
              migrator.migrate
            end
    
            it "should update schema to 0" do
              @mock_hbase_connection.should_receive(:update_schema_version).with('user', 'env',0)
          
              migrator = HbaseMigrations::Migrator.new(:down,'spec/data', 'server', 'user', 'env')
              migrator.migrate
            end
        end
        
        describe "and target version is 1" do
            it "should call :down method only in test script 2 and not in test script 2" do
              TestScript1.should_not_receive(:send).with(:down)
              TestScript2.should_receive(:send).with(:down)
          
              migrator = HbaseMigrations::Migrator.new(:down,'spec/data', 'server', 'user', 'env',1)
              migrator.migrate
            end
    
            it "should update schema to 1" do
              @mock_hbase_connection.should_receive(:update_schema_version).with('user', 'env',1)
          
              migrator = HbaseMigrations::Migrator.new(:down,'spec/data', 'server', 'user', 'env',1)
              migrator.migrate
            end
        end
      end
    end
    
end


describe HbaseMigrations::Migrator, " when pending_migrations is called " do
    
    before(:each) do
        @mock_hbase_connection = mock('hbase_connection')
        @mock_hbase_connection.stub!(:initialize_schema_information)
        @mock_hbase_connection.stub!(:update_schema_version)
        HbaseRecord::Base.stub!(:establish_connection).and_return(@mock_hbase_connection)
    end
    
    describe " and current version is 0" do
      before(:each) do
          HbaseMigrations::Migrator.stub!(:current_version).and_return(0)    
      end
      
      it "should return both TestScript 1 and TestScript 2" do
         pending_migrations = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',1).pending_migrations
         pending_migrations.should have_at_most(2).things
         pending_migrations.should == [TestScript1,TestScript2]
      end
    end
    
    describe " and current version is 1" do
      before(:each) do
          HbaseMigrations::Migrator.stub!(:current_version).and_return(1)
      end
      
      it "should return only TestScript 2" do
          pending_migrations = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',1).pending_migrations
          pending_migrations.should have_at_most(1).things
          pending_migrations.should == [TestScript2]
      end
    end

    describe " and current version is 2" do
      before(:each) do
          HbaseMigrations::Migrator.stub!(:current_version).and_return(2)
      end
      
      it "should return empty" do
          pending_migrations = HbaseMigrations::Migrator.new(:up,'spec/data', 'server', 'user', 'env',1).pending_migrations
          pending_migrations.should have_at_most(0).things
          pending_migrations.should == []
      end
    end   
    
end