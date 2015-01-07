require 'spec_helper'

# Time mocking features are available in Rails 4 but not Rails 3 and the Timecop 
# gem works with both.
require 'timecop'

describe CleanSweep::PurgeRunner do

  context 'PurgeRunner' do
    before do
      Timecop.freeze Time.parse("2014-12-02 13:47:43.000000 -0800")
    end
    after do
      Timecop.return
    end

    context "using comments" do
      before do
        Comment.create_table
      end
      context "with duplicate rows" do

        # This testcase demonstrates a weakness in the index traversal
        # which is that if you aren't using a unique index or the first_only option,
        # you can miss rows.
        #
        # In this case we have some duplicate rows but because the chunk_size is
        # set low, we don't get all the duplicates in one chunk.  And they miss
        # the next chunk because we are looking for values greater than the
        # columns in the current chunk.
        #
        # If you use the first_only option it means it builds the where clause using only
        # the first column of the index, and it also uses the >=, <= operators instead
        # of >, <.  So it picks up all the rows.
        #

        before do
          10.times { create(:comment, timestamp: 2.weeks.ago, seen: false) }
          10.times { create(:comment, timestamp: 2.weeks.ago, seen: false) }
          10.times { create(:comment, timestamp: 2.days.ago, seen: false) }
          10.times { create(:comment, timestamp: 2.days.ago, seen: false) }
          10.times { create(:comment, timestamp: 2.days.ago, seen: false) }
        end

        it "can miss some rows" do
          purger = CleanSweep::PurgeRunner.new model: Comment,
                                               index: 'comments_on_timestamp',
                                               chunk_size: 7 do | scope |
            scope.where('timestamp < ?', 1.week.ago)
          end
          expect( -> {
            purger.execute_in_batches
          }).to change(Comment, :count).from(50).to(43)  # if it deleted all dups this would be 30, not 42
        end
        it "won't miss rows using first_only option" do
          purger = CleanSweep::PurgeRunner.new model: Comment,
                                               index: 'comments_on_timestamp',
                                               first_only: true,
                                               chunk_size: 7 do | scope |
            scope.where('timestamp < ?', 1.week.ago)
          end
          expect( -> {
            purger.execute_in_batches
          }).to change(Comment, :count).from(50).to(30)  # if it deleted all dups this would be 30, not 42

        end

        it 'prints out the queries in a dry run' do
          purger = CleanSweep::PurgeRunner.new model: Comment,
                                               index: 'comments_on_account_timestamp'  do | scope |
            scope.where('timestamp < ?', 1.week.ago.to_date)
          end
          output = purger.print_queries
          expect(output).to eq <<EOF
Initial Query:
    SELECT  `comments`.`id`,`comments`.`account`,`comments`.`timestamp`
    FROM `comments` FORCE INDEX(comments_on_account_timestamp)
    WHERE (timestamp < '2014-11-25')
    ORDER BY `comments`.`account` ASC,`comments`.`timestamp` ASC
    LIMIT 500
Chunk Query:
    SELECT  `comments`.`id`,`comments`.`account`,`comments`.`timestamp`
    FROM `comments` FORCE INDEX(comments_on_account_timestamp)
    WHERE (timestamp < '2014-11-25') AND (`comments`.`account` > 0 OR (`comments`.`account` = 0 AND `comments`.`timestamp` > '2014-11-18'))\n    ORDER BY `comments`.`account` ASC,`comments`.`timestamp` ASC
    LIMIT 500
Delete Statement:
    DELETE
    FROM `comments`
    WHERE (`comments`.`id` = 2)
EOF
        end
      end
      context "with unique rows" do
        before do
          # Create 10 comments going back 0..9 days...
          10.times { |i| create(:comment, timestamp: i.days.ago) }
        end

        it "ascends the index" do
          purger = CleanSweep::PurgeRunner.new model: Comment,
                                               index: 'comments_on_timestamp',
                                               stop_after: 5
          begin
            purger.execute_in_batches
          rescue CleanSweep::PurgeStopped
          end
          expect(Comment.count).to eq(5)
          # Only old comments deleted before stopping
          expect(Comment.where('timestamp >= ?', 4.days.ago.to_date).count).to eq(5)
        end
        it "descends the index" do
          purger = CleanSweep::PurgeRunner.new model: Comment,
                                               index: 'comments_on_timestamp',
                                               reverse: true,
                                               stop_after: 5
          begin
            purger.execute_in_batches
          rescue CleanSweep::PurgeStopped
          end
          # Delete from the most recent comments, so only old ones are left.
          expect(Comment.count).to eq(5)
          expect(Comment.where('timestamp <= ?', 4.days.ago).count).to eq(5)

        end
      end
    end


    context "using books" do

      before do
        @total_book_size = 50
        Book.create_table
        @total_book_size.times { create(:book) }
      end

      after do
        Book.delete_all
      end

      it 'waits for history' do
        purger = CleanSweep::PurgeRunner.new model: Book,
                                             max_history: 100,
                                             chunk_size: 10
        mysql_status = purger.mysql_status
        expect(mysql_status).to receive(:check!).exactly(6).times

        purger.execute_in_batches

      end

      it 'should not check when there are no limits' do
        purger = CleanSweep::PurgeRunner.new model: Book,
                                             chunk_size: 4

        expect(purger.mysql_status).to be_nil
      end

      it 'purges books' do
        purger = CleanSweep::PurgeRunner.new model: Book,
                                             chunk_size: 4

        count = purger.execute_in_batches
        expect(count).to be(@total_book_size)
        expect(Book.count).to be 0
      end

      it 'copies books' do
        BookTemp.create_table
        purger = CleanSweep::PurgeRunner.new model: Book,
                                             copy_columns: ['publisher'],
                                             dest_model: BookTemp,
                                             dest_columns: { 'PUBLISHER' => 'published_by', 'ID' => 'book_id'},
                                             chunk_size: 4,
                                             copy_only: true,
                                             index: 'book_index_by_bin'

        count = purger.execute_in_batches
        expect(count).to be(@total_book_size)
        expect(BookTemp.count).to eq(@total_book_size)
        last_book = BookTemp.last
        expect(last_book.book_id).to be 200
        expect(last_book.bin).to be 2000
        expect(last_book.published_by).to eq 'Random House'
      end

    end
  end
end

describe CleanSweep::PurgeRunner::MysqlStatus do

  context "mysql status check tool" do

    let(:mysql_status) do
      CleanSweep::PurgeRunner::MysqlStatus.new model: Book, max_history:100, max_repl_lag: 100
    end

    before do
      Book.create_table
    end

    it "fetches innodb status" do
      mysql_status.get_replication_lag
    end
    it "checks history and pauses" do
      allow(mysql_status).to receive(:get_history_length).and_return(101, 95, 89)
      expect(mysql_status).to receive(:pause).twice
      mysql_status.check!
    end
    it "checks replication and pauses" do
      allow(mysql_status).to receive(:get_replication_lag).and_return(101, 95, 89)
      expect(mysql_status).to receive(:pause).twice
      mysql_status.check!
    end

    it "checks and continues" do
      allow(mysql_status).to receive(:get_history_length).and_return(80)
      expect(mysql_status).not_to receive(:pause)
      mysql_status.check!
    end

    it "fetches slave status" do
      mysql_status.get_history_length
    end
  end

end


