
class CleanSweep::TableSchema

  # The list of columns used when selecting, the union of pk and descending index columns
  attr_reader :select_columns

  # The schema for the primary key
  attr_reader :primary_key

  # The schema for the descending key, or nil
  attr_reader :descending_key

  # Is the descending key optional?
  def initialize(model, descending_key_name=nil)
    @model = model
    @select_columns = []

    key_schemas = build_indexes

    # Primary key only supported, but we could probably get around this by adding
    # all columns as 'primary key columns'
    raise "Table #{model.table_name} must have a primary key" unless key_schemas.include? 'PRIMARY'

    @primary_key = key_schemas['PRIMARY']
    @primary_key.add_columns_to @select_columns
    if descending_key_name
      descending_key_name.upcase!
      raise "BTREE Index #{descending_key_name} not found" unless key_schemas.include? descending_key_name
      @descending_key = key_schemas[descending_key_name]
      @descending_key.add_columns_to @select_columns
    end
  end


  private

  def build_indexes
    indexes = {}
    column_details = @model.connection.select_rows "show indexes from #{@model.quoted_table_name}"
    column_details.each do | col |
      key_name = col[2].upcase
      col_name = col[4].upcase
      ascending = col[5] == 'A'
      type = col[10]
      next if key_name != 'PRIMARY' && type != 'BTREE'  # Only BTREE indexes supported for descending
      indexes[key_name] ||= IndexSchema.new key_name, ascending
      indexes[key_name] << col_name
    end
    return indexes
  end

  ColumnSchema = Struct.new :name, :select_position

  class IndexSchema < Struct.new :name, :ascending

    attr_reader :columns

    def initialize *args
      super
      @columns = []
    end

    def << col_name
      @columns << ColumnSchema.new(col_name)
    end

    def add_columns_to select_columns
      @columns.each do | column |
        pos = select_columns.index column.name
        if pos.nil?
          select_columns << column.name
          pos = select_columns.size - 1
        end
        column.select_position = pos
      end
    end
  end
end

