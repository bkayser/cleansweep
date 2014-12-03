
class CleanSweep::TableSchema

  # The list of columns used when selecting, the union of pk and traversing key columns
  attr_reader :columns

  # The schema for the primary key
  attr_reader :primary_key

  # The schema for the traversing key, or nil
  attr_reader :traversing_key

  attr_reader :name

  def initialize(model, options={})

    traversing_key_name  = options[:key_name]
    ascending            = options.include?(:ascending) ? options[:ascending] : true
    first_only           = options[:first_only]
    @model               = model
    @dest_model          = options[:dest_model] || @model

    # Downcase and symbolize the entries in the column name map:
    dest_columns_map     = Hash[*(options[:dest_columns] || {}).to_a.flatten.map{|n| n.to_s.downcase.to_sym}]

    @name                = @model.table_name

    @columns      =
      (options[:extra_columns] || []).map do | extra_col_name |
        CleanSweep::TableSchema::ColumnSchema.new extra_col_name, model
      end

    key_schemas = build_indexes

    # Primary key only supported, but we could probably get around this by adding
    # all columns as 'primary key columns'
    raise "Table #{model.table_name} must have a primary key" unless key_schemas.include? 'primary'

    @primary_key = key_schemas['primary']
    @primary_key.add_columns_to @columns
    if traversing_key_name
      traversing_key_name.downcase!
      raise "BTREE Index #{traversing_key_name} not found" unless key_schemas.include? traversing_key_name
      @traversing_key = key_schemas[traversing_key_name]
      @traversing_key.add_columns_to @columns
      @traversing_key.ascending = ascending
      @traversing_key.first_only = first_only
    end

    # Specify the column names in the destination map, if provided
    @columns.each do | column |
      column.dest_name = dest_columns_map[column.name]
    end

  end

  def column_names
    @columns.map(&:name)
  end

  def insert_statement(rows)
    "insert into #{@dest_model.quoted_table_name} (#{quoted_dest_column_names}) values #{quoted_row_values(rows)}"
  end

  def delete_statement(rows)
    rec_criteria = rows.map do | row |
      row_compares = []
      @primary_key.columns.each do |column|
        row_compares << "#{column.quoted_dest_name(@dest_model)} = #{column.quoted_value(row)}"
      end
      "(" + row_compares.join(" AND ") + ")"
    end
    "DELETE FROM #{@dest_model.quoted_table_name} WHERE #{rec_criteria.join(" OR ")}"
  end

  def initial_scope
    scope = @model.all.select(quoted_column_names).from(from_clause)
    scope = @traversing_key.order(scope) if @traversing_key
    return scope
  end

  def scope_to_next_chunk scope, last_row
    if @traversing_key.blank?
      scope
    else
      @traversing_key.scope_to_next_chunk(scope, last_row)
    end
  end

  def first_only?
    @traversing_key && @traversing_key.first_only
  end

  private

  def from_clause
    table_name = @model.quoted_table_name
    table_name += " FORCE INDEX(#{@traversing_key.name})" if @traversing_key
    return table_name
  end

  def quoted_column_names
    columns.map{|c| "#{c.quoted_name}"}.join(",")
  end

  def quoted_dest_column_names
    columns.map{|c| c.quoted_dest_name(@dest_model)}.join(",")
  end

  def quoted_row_values(rows)
    rows.map do |vec|
      row = []
      columns.each_with_index do | col, i |
        row << @model.quote_value(vec[i], col.ar_column)
      end
      "(#{row.join(',')})"
    end.join(",")
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

end

