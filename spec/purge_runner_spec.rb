require 'spec_helper'

describe CleanSweep::PurgeRunner do
  context 'PurgeRunner' do

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
                                             keys: %w[bin id],
                                             max_history: 100,
                                             limit: 10
        mysql_status = purger.mysql_status
        expect(mysql_status).to receive(:check!).exactly(6).times

        purger.execute_in_batches

      end

      it 'should not check when there are no limits' do
        purger = CleanSweep::PurgeRunner.new model: Book,
                                             keys: %w[bin id],
                                             limit: 4

        expect(purger.mysql_status).to be_nil
      end

      it 'purges books' do
        purger = CleanSweep::PurgeRunner.new model: Book,
                                             keys: %w[bin id],
                                             limit: 4

        count = purger.execute_in_batches
        expect(count).to be(@total_book_size)
        expect(Book.count).to be 0
      end

      it 'copies books' do
        BookTemp.create_table
        purger = CleanSweep::PurgeRunner.new source: Book,
                        target: BookTemp,
                        keys: %w[bin id],
                        limit: 4,
                        copy: true,
                        index: 'book_index_by_bin'

        count = purger.execute_in_batches
        expect(count).to be(@total_book_size)
        expect(BookTemp.count).to be(@total_book_size)
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


class BookTemp < ActiveRecord::Base

  self.table_name = 'book_vault'

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    book_vault (
       `id` int(11) auto_increment,
       `bin` int(11),
       primary key (bin, id)
    )
    EOF
  end
end
