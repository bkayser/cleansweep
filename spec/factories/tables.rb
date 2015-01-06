class TableWithPrimaryKey < ActiveRecord::Base

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    table_with_primary_keys (
       `pk` int(11) primary key auto_increment,
       `k1` int(11),
       `k2` int(11),
       key key_nonunique (k1),
       unique key key_unique (k2)
    )
    EOF
  end

end

class TableWithUniqueKey < ActiveRecord::Base

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    table_with_unique_keys (
       `k1` int(11),
       `k2` int(11),
       key key_nonunique (k1),
       unique key key_unique (k2)
    )
    EOF
  end

end

class TableWithRegularKey < ActiveRecord::Base

  def self.create_table
    connection.execute <<-EOF
    create temporary table if not exists
    table_with_regular_keys (
       `k1` int(11),
       `k2` int(11),
        key key_nonunique (k1),
        key key_extra (k2)
    )
    EOF
  end

end

