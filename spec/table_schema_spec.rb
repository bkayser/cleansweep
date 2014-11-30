require 'spec_helper'

describe CleanSweep::TableSchema do

  before do
    Comment.create_table
  end

  it 'should read comments' do
    schema = CleanSweep::TableSchema.new Comment, 'COMMENTS_ON_TIMESTAMP'
    expect(schema.primary_key.columns.map(&:name)).to eq(%w[ID])
    expect(schema.descending_key.columns.map(&:name)).to eq(%w[ACCOUNT TIMESTAMP])
  end

  it 'should not care about case' do
    CleanSweep::TableSchema.new Comment, 'primary'
  end

  it 'should work without a descending index' do
    schema = CleanSweep::TableSchema.new Comment
    expect(schema.primary_key.columns.map(&:name)).to eq(%w[ID])
    expect(schema.descending_key).to be_nil
  end


end
