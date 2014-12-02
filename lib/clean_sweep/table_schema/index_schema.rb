class CleanSweep::TableSchema::IndexSchema < Struct.new :name, :model, :ascending

  attr_accessor :columns, :name, :model, :ascending, :first_only

  def initialize name, model
    @model = model
    @columns = []
    @name = name
  end

  # Add a column
  def << col_name
    @columns << CleanSweep::TableSchema::ColumnSchema.new(col_name, model)
  end

  # Take columns referenced by this index and add them to the list if they
  # are not present.  Record their position in the list because the position will
  # be where they are located in a row of values passed in later to #scope_to_next_chunk
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

  def order(scope)
    direction = ascending ? 'ASC' : 'DESC'
    if @first_only
      scope.order("#{columns.first.quoted_name} #{direction}")
    else
      scope.order(columns.map { |col| "#{col.quoted_name} #{direction}"}.join(","))
    end
  end

  def scope_to_next_chunk(scope, last_row)
    query_args = {}
    if @first_only
      query_args[columns.first.name] = columns.first.value(last_row)
    else
      columns.each do |column|
        query_args[column.name] = column.value(last_row)
      end
    end
    scope.where(chunk_clause, query_args)
  end

  private

  def chunk_clause
    @chunk_clause ||=
        if @first_only
          # If we're only using the first column, you have to do an inclusive comparison
          "#{columns.first.quoted_name} #{ascending ? ">=" : "<="} :#{columns.first.name}"
        else
          # If you are using all columns of the index, build the expression recursively
          add_term(columns.dup)
        end
  end

  def add_term(columns)
    column = columns.shift
    clause = "#{column.quoted_name} #{ascending ? ">" : "<"} :#{column.name}"
    if columns.any?
      clause << " OR (#{column.quoted_name} = :#{column.name} AND #{add_term columns})"
    end
    return clause
  end
end