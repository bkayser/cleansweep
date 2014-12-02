class CleanSweep::TableSchema::ColumnSchema

  attr_reader :name
  attr_accessor :select_position

  def initialize(name, model)
    @name = name.to_sym
    col_num = model.column_names.index(name.to_s) or raise "Can't find #{name} in #{model.name}"
    @model = model
    @column = model.columns[col_num]
  end

  def quoted_name
    "`#{name}`"
  end
  def value(row)
    row[select_position]
  end
  def quoted_value(row)
    @model.quote_value(value(row), @column)
  end
end

