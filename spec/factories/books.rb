class Book < ActiveRecord::Base

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    books (
       `id` int(11) auto_increment,
       `bin` int(11),
       `publisher` varchar(64),
       `title` varchar(64),
       primary key (id),
       key book_index_by_bin(bin, id)
    )
    EOF
    Book.delete_all
  end

end

FactoryGirl.define do
  factory :book do | book |
    book.publisher "Random House"
    book.sequence(:bin) { | n | (n % 3)  * 1000 }
    book.sequence(:title) { |n|  "Jaws, Part #{n}"}
  end
end

class BookTemp < ActiveRecord::Base

  self.table_name = 'book_vault'
  self.primary_key= 'book_id'

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    book_vault (
       `book_id` int(11) primary key auto_increment,
       `bin` int(11),
       `published_by` varchar(64)
    )
    EOF
    BookTemp.delete_all
  end
end
