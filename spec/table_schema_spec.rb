require 'spec_helper'

describe CleanSweep::TableSchema do

  before do
    Comment.create_table
  end

  context "using ascending account, timestamp index" do
    let(:schema) { CleanSweep::TableSchema.new Comment, key_name:'comments_on_account_timestamp', ascending: true }

    it 'should read comments' do
      expect(schema.primary_key.columns.map(&:name)).to eq([:id])
      expect(schema.traversing_key.columns.map(&:name)).to eq([:account, :timestamp])
    end

    it 'should produce an ascending chunk clause' do
      rows = account_and_timestamp_rows
      expect(schema.scope_to_next_chunk(schema.initial_scope, rows.last).to_sql)
          .to include("(`account` > 5 OR (`account` = 5 AND `timestamp` > '2014-12-01 23:13:25'))")
    end

    it 'should produce all select columns' do
      expect(schema.column_names).to eq([:id, :account, :timestamp])
    end

    it 'should produce the ascending order clause' do
      expect(schema.initial_scope.to_sql).to include('`account` ASC,`timestamp` ASC')
    end


    it 'should produce an insert statement' do
      schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp'
      rows = account_and_timestamp_rows
      expect(schema.insert_statement(rows)).to eq("insert into `comments` (`comments`.`id`,`comments`.`account`,`comments`.`timestamp`) values (1001,5,'2014-12-02 01:13:25'),(1002,2,'2014-12-02 00:13:25'),(1005,5,'2014-12-01 23:13:25')")
    end
  end

  context "using descending account, timestamp index" do

    let(:schema) { CleanSweep::TableSchema.new Comment, key_name:'comments_on_account_timestamp', ascending: false }

    it 'should produce a descending where clause' do
      rows = account_and_timestamp_rows
      expect(schema.scope_to_next_chunk(schema.initial_scope, rows.last).to_sql)
          .to include("(`account` < 5 OR (`account` = 5 AND `timestamp` < '2014-12-01 23:13:25'))")
    end


    it 'should produce the descending order clause' do
      rows = account_and_timestamp_rows
      expect(schema.scope_to_next_chunk(schema.initial_scope, rows.last).to_sql)
          .to include("`account` DESC,`timestamp` DESC")
    end

  end

  context "using account, timestamp index first column only" do
    let(:schema) { CleanSweep::TableSchema.new Comment, key_name:'comments_on_account_timestamp', first_only: true }

    it 'should select all the rows' do
      expect(schema.column_names).to eq([:id, :account, :timestamp])
    end

    it 'should only query using the first column of the index' do
      rows = account_and_timestamp_rows
      expect(schema.scope_to_next_chunk(schema.initial_scope, rows.last).to_sql)
        .to include(" (`account` >= 5) ")

    end

  end

  it 'should not care about case' do
    CleanSweep::TableSchema.new Comment, key_name: 'primary'
  end

  it 'should work without a descending index' do
    schema = CleanSweep::TableSchema.new Comment
    expect(schema.primary_key.columns.map(&:name)).to eq([:id])
    expect(schema.traversing_key).to be_nil
  end

  it 'should produce minimal select columns' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'PRIMARY'
    expect(schema.column_names).to eq([:id])
  end

  it 'should produce the from clause with an index' do
    schema = CleanSweep::TableSchema.new Comment, key_name:'comments_on_timestamp'
    expect(schema.initial_scope.to_sql).to include("`comments` FORCE INDEX(comments_on_timestamp)")
  end

  it 'should include additional columns' do
    schema = CleanSweep::TableSchema.new Comment, key_name: 'comments_on_account_timestamp', extra_columns: %w[seen id]
    expect(schema.column_names).to eq([:seen, :id, :account, :timestamp])
    rows = account_and_timestamp_rows
    rows.map! { |row| row.unshift 1 } # Insert 'seen' value to beginning of row
    expect(schema.insert_statement(rows)).to eq("insert into `comments` (`comments`.`seen`,`comments`.`id`,`comments`.`account`,`comments`.`timestamp`) values (1,1001,5,'2014-12-02 01:13:25'),(1,1002,2,'2014-12-02 00:13:25'),(1,1005,5,'2014-12-01 23:13:25')")

  end


  def account_and_timestamp_rows
    rows = []
    t = Time.parse '2014-12-01 17:13:25'
    rows << [1001, 5, t]
    rows << [1002, 2, t - 1.hour]
    rows << [1005, 5, t - 2.hours]
  end
end
