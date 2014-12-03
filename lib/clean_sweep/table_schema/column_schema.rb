class CleanSweep::TableSchema::ColumnSchema

  attr_reader :name, :ar_column
  attr_accessor :select_position
  attr_writer :dest_name

  def initialize(name, model)
    @name = name.to_sym
    col_num = model.column_names.index(name.to_s) or raise "Can't find #{name} in #{model.name}"
    @model = model
    @ar_column = model.columns[col_num]
  end

  def quoted_name
    "`#{name}`"
  end

  def quoted_dest_name
    "`#{@dest_name || @name}`"
  end

  def value(row)
    row[select_position]
  end

  def quoted_value(row)
    @model.quote_value(value(row), @ar_column)
  end

  def == other
    return other && name == other.name
  end
end

