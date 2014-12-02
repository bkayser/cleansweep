
class CleanSweep::TableSchema

  # The list of columns used when selecting, the union of pk and traversing key columns
  attr_reader :select_columns

  # The schema for the primary key
  attr_reader :primary_key

  # The schema for the traversing key, or nil
  attr_reader :traversing_key

  attr_reader :name

  def initialize(model, options={})

    traversing_key_name  = options[:key_name]
    ascending            = options.include?(:ascending) ? options[:ascending] : true
    @model               = model
    @name                = @model.table_name
    @select_columns      = (options[:extra_columns] && options[:extra_columns].map(&:to_sym)) || []

    key_schemas = build_indexes

    # Primary key only supported, but we could probably get around this by adding
    # all columns as 'primary key columns'
    raise "Table #{model.table_name} must have a primary key" unless key_schemas.include? 'primary'

    @primary_key = key_schemas['primary']
    @primary_key.add_columns_to @select_columns
    if traversing_key_name
      traversing_key_name.downcase!
      raise "BTREE Index #{traversing_key_name} not found" unless key_schemas.include? traversing_key_name
      @traversing_key = key_schemas[traversing_key_name]
      @traversing_key.add_columns_to @select_columns
      @traversing_key.ascending = ascending
    end
  end

  def insert_statement(target_model, rows)
    "insert into #{target_model.quoted_table_name} (#{quoted_column_names}) values #{quoted_row_values(rows)}"
  end

  def delete_statement(rows)
    rec_criteria = rows.map do | row |
      row_compares = []
      @primary_key.columns.each do |column|
        row_compares << "#{column.quoted_name} = #{column.quoted_value(row)}"
      end
      "(" + row_compares.join(" AND ") + ")"
    end
    "DELETE FROM #{@model.quoted_table_name} WHERE #{rec_criteria.join(" OR ")}"
  end

  def initial_scope
    scope = @model.all.select(quoted_column_names).from(from_clause)
    scope = scope.order(order_clause) if @traversing_key
    return scope
  end

  def scope_to_next_chunk scope, last_row
    return scope if @traversing_key.blank?
    query_args = {}
    @traversing_key.columns.each do |column|
      query_args[column.name] = column.value(last_row)
    end
    scope.where(chunk_clause, query_args)
  end

  private

  def from_clause
    table_name = @model.quoted_table_name
    table_name += " FORCE INDEX(#{@traversing_key.name})" if @traversing_key
    return table_name
  end

  def order_clause
    @traversing_key.columns.map { |col| "#{col.quoted_name} #{@traversing_key.ascending ? 'ASC' : 'DESC'}"}.join(",")
  end

  def quoted_column_names
    select_columns.map{|c| "`#{c}`"}.join(",")
  end


  def quoted_row_values(rows)
    rows.map do |vec|
      quoted_column_values = vec.map do |col_value|
        @model.connection.quote(col_value)
      end.join(",")
      "(#{quoted_column_values})"
    end.join(",")
  end

  def chunk_clause
    return if @traversing_key.nil?
    @chunk_clause ||= add_term(@traversing_key.columns.dup, @traversing_key.ascending)
  end

  def add_term(columns, ascending)
    column = columns.shift
    clause = "#{column.quoted_name} #{ascending ? ">" : "<"} :#{column.name}"
    if columns.any?
      clause << " OR (#{column.quoted_name} = :#{column.name} AND #{add_term columns, ascending})"
    end
    return clause
  end


  def build_indexes
    indexes = {}
    column_details = @model.connection.select_rows "show indexes from #{@model.quoted_table_name}"
    column_details.each do | col |
      key_name = col[2].downcase
      col_name = col[4].downcase
      type = col[10]
      next if key_name != 'PRIMARY' && type != 'BTREE'  # Only BTREE indexes supported for traversing
      indexes[key_name] ||= IndexSchema.new key_name, @model
      indexes[key_name] << col_name
    end
    return indexes
  end

  class ColumnSchema

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

  class IndexSchema < Struct.new :name, :model, :ascending

    attr_reader :columns

    def initialize *args
      super
      @columns = []
    end

    def << col_name
      @columns << ColumnSchema.new(col_name, model)
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

