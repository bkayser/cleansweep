require 'spec_helper'

describe CleanSweep::TableSchema do

  before do
    Comment.create_table
  end

  it 'should read comments' do
    schema = CleanSweep::TableSchema.new Comment, key_name:'comments_on_account_timestamp'
    expect(schema.primary_key.columns.map(&:name)).to eq([:id])
    expect(schema.traversing_key.columns.map(&:name)).to eq([:account, :timestamp])
  end

  it 'should not care about case' do
    CleanSweep::TableSchema.new Comment, key_name: 'primary'
  end

  it 'should work without a descending index' do
    schema = CleanSweep::TableSchema.new Comment
    expect(schema.primary_key.columns.map(&:name)).to eq([:id])
    expect(schema.traversing_key).to be_nil
  end

  it 'should produce an ascending chunk clause' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp', ascending:true
    expect(schema.send :chunk_clause).to eq("`account` > :account OR (`account` = :account AND `timestamp` > :timestamp)")
  end

  it 'should produce all select columns' do
    schema = CleanSweep::TableSchema.new Comment, key_name:'comments_on_account_timestamp', ascending:true
    expect(schema.select_columns).to eq([:id, :account, :timestamp])
  end

  it 'should produce minimal select columns' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'PRIMARY'
    expect(schema.select_columns).to eq([:id])
  end

  it 'should produce a descending chunk clause' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp', ascending: false
    expect(schema.send :chunk_clause).to eq("`account` < :account OR (`account` = :account AND `timestamp` < :timestamp)")
  end

  it 'should produce the from clause with an index' do
    schema = CleanSweep::TableSchema.new Comment, key_name:'comments_on_timestamp'
    expect(schema.initial_scope.to_sql).to include("`comments` FORCE INDEX(comments_on_timestamp)")
  end

  it 'should produce the ascending order clause' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp', ascending:true
    expect(schema.initial_scope.to_sql).to include('`account` ASC,`timestamp` ASC')
  end

  it 'should produce the descending order clause' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp', ascending:false
    expect(schema.initial_scope.to_sql).to include('`account` DESC,`timestamp` DESC')
  end

  it 'should produce an insert statement' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp'
    t = Time.parse '2014-12-01 17:13:25'
    rows = []
    rows << [1001, 5, t]
    rows << [1002, 2, t - 1.hour]
    rows << [1005, 5, t - 2.hours]
    expect(schema.insert_statement(Comment, rows)).to eq("insert into `comments` (`id`,`account`,`timestamp`) values (1001,5,'2014-12-02 01:13:25'),(1002,2,'2014-12-02 00:13:25'),(1005,5,'2014-12-01 23:13:25')")
  end

  it 'should include additional columns' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp', extra_columns: %w[seen id]
    expect(schema.select_columns).to eq([:seen, :id, :account, :timestamp])
    t = Time.parse '2014-12-01 17:13:25'
    rows = []
    rows << [1, 1001, 5, t]
    rows << [1, 1002, 2, t - 1.hour]
    rows << [0, 1005, 5, t - 2.hours]
    expect(schema.insert_statement(Comment, rows)).to eq("insert into `comments` (`seen`,`id`,`account`,`timestamp`) values (1,1001,5,'2014-12-02 01:13:25'),(1,1002,2,'2014-12-02 00:13:25'),(0,1005,5,'2014-12-01 23:13:25')")

  end
end
