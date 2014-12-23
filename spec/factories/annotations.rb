
# Defines a table that does not have a primary key but does
# have a unique key.
class Annotation < ActiveRecord::Base

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    annotations (
       `article_id` int(11) NOT NULL,
       `text` varchar(64),
       key `index_on_text` (`text`),
       unique key (`article_id`)
    )
    EOF
    Annotation.delete_all
  end

end
