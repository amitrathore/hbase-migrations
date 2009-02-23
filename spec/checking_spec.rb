require File.dirname(__FILE__) + '/spec_helper'

describe Checking do
    
    it "should return 1" do
      Checking.new.give.should == 1
    end
  
end
