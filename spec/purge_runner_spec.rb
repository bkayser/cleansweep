require 'spec_helper'

describe CleanSweep::PurgeRunner do
  context 'PurgeRunner' do

    context "using comments" do
      before do
        Comment.create_table
      end
      context "with duplicate rows" do

        # This testcase demonstrates a weakness in the index traversal
        # which is that if you aren't using a unique index, you can miss rows.
        # In this case we have some duplicate rows but because the chunk_size is
        # set low, we don't get all the duplicates in one chunk.  And they miss
        # the next chunk because we are looking for values greater than the
        # columns in the current chunk.
        #
        # If we did an inclusive comparison it would fix the problem but it would also
        # mean copying rows more than once, or getting in an infinite loop on
        # dry_run.
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
                                               index: 'comments_on_account_timestamp',
                                               chunk_size: 4 do | scope |
            scope.where('timestamp < ?', 1.week.ago)
          end
          expect( -> {
            purger.execute_in_batches
          }).to change(Comment, :count).from(50).to(38)  # if it deleted all dups this would be 30, not 42
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
          expect(Comment.where('timestamp >= ?', 4.days.ago).count).to eq(5)
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
                                             dest_model: BookTemp,
                                             chunk_size: 4,
                                             index: 'book_index_by_bin'

        count = purger.execute_in_batches
        expect(count).to be(@total_book_size)
        expect(BookTemp.count).to eq(@total_book_size)
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


