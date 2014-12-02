class Comment < ActiveRecord::Base

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    comments (
       `id` int(11) primary key auto_increment,
       `timestamp` datetime,
       `account` int(11),
       `seen` boolean,
       key comments_on_account_timestamp(account, timestamp),
       key comments_on_timestamp(timestamp desc)
    )
    EOF
    connection.execute 'truncate table comments'
  end

end

FactoryGirl.define do
  factory :comment do | comment |
    comment.timestamp Time.now
    comment.seen false
    comment.sequence(:account) { | n | (n % 3)* 100 }
  end
end
