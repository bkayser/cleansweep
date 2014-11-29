# This is a utility built to mimic some of the features of the pt_archive script
# to make really big purges go faster with no production impact.
#
# It uses a strategy of descending an index, querying for purgeable ids and then
# deleting them in batches.
#
#
# options:
#
# * source
# * target
# * model
# * source_keys
# * target_keys
# * keys
# * limit
# * stop_after
# * report
# * logger
# * copy
# * dry_run
# * sleep
# * index
# * max_history
# * max_replication
#
class CleanSweep::PurgeRunner

  require 'clean_sweep/purge_runner/logging'
  require 'clean_sweep/purge_runner/mysql_status'

  include CleanSweep::PurgeRunner::Logging

  # This helps us track the state of replication and history list and pause
  # if necessary
  attr_reader :mysql_status
  
  def initialize(options={})
    @source_model     = options[:source] || options[:model] or raise "source model class required"
    @target_model     = options[:target] || options[:model]
    @source_keys      = Array(options[:keys]||options[:source_keys]).map &:to_sym
    @target_keys      = options.include?(:target_keys) ? Array(options[:target_keys]).map(&:to_sym) : @source_keys
    @key_column_names = @source_keys.map{ |key| "#{@source_model.quoted_table_name}.#{key}"}
    @limit            = options[:limit] or raise "limit required"
    @stop_after       = options[:stop_after]
    @report_interval  = options[:report] || 10.seconds
    @logger           = options[:logger] || ActiveRecord::Base.logger || Logger.new($stdout)
    @copy             = options[:copy]
    @max_history      = options[:max_history]
    @max_repl_lag     = options[:max_repl_lag]

    if (@max_history || @max_repl_lag)
      @mysql_status = CleanSweep::PurgeRunner::MysqlStatus.new model: @source_model,
                                                               max_history: @max_history,
                                                               max_repl_lag: @max_repl_lag,
                                                               check_period: options[:check_period],
	                                                       logger: @logger
    end

    raise "You can't copy rows from a table into itself" if copy_mode? && @source_model == @target_model

    @report_interval_start = Time.now

    @index_entrypoint = case @source_keys.size
                          when 2
                            "#{@key_column_names.first} > :#{@source_keys.first} OR " +
                                "(#{@key_column_names.first} = :#{@source_keys.first} and #{@key_column_names.second} > :#{@source_keys.second})"
                          when 1
                            "#{@key_column_names.first} > :#{@source_keys.first}"
                          else
                            raise "one or two keys only required for index descent"
                        end

    from              = @source_model.quoted_table_name
    from             += " FORCE INDEX(#{options[:index]})" if options[:index]
    scope             = block_given? ? yield(@source_model) : @source_model.all
    @query            = scope.select(@key_column_names).from(from).limit(@limit) #.order(@key_column_names.join(","))
    @dry_run          = options[:dry_run]
    @sleep            = options[:sleep]
  end


  def copy_mode?
    @copy
  end

  # Execute the purge and return true if we hit the stop after limit
  def execute_in_batches

    @start = Time.now
    if copy_mode?
      verb = "copying"
      log :info,  "#{'DRY RUN ' if @dry_run}starting: copying #{@source_model.table_name} records to #{@target_model.table_name} in batches of #{@limit}"
    else
      verb = "purging"
      log :info,  "#{'DRY RUN ' if @dry_run}starting: deleting #{@target_model.table_name} records in batches of #{@limit}"
    end

    log :info,  "sleeping #{@sleep} seconds between purging" if @sleep && !copy_mode?
    @total_deleted = 0
    # Iterate through the rows in limit chunks
    log :debug, "find rows: #{@query.to_sql}" if @logger.level == Logger::DEBUG

    @mysql_status.check! if @mysql_status

    rows = NewRelic::Agent.with_database_metric_name(@source_model, 'SELECT') do
      @source_model.connection.select_rows @query.to_sql
    end
    while rows.any? && (!@stop_after || @total_deleted < @stop_after) do
      index_entrypoint_args = Hash[*@source_keys.zip(rows.last).flatten]
      log :debug, "#{verb} #{rows.size} records between #{rows.first.inspect} and #{rows.last.inspect}" if @logger.level == Logger::DEBUG
      stopped = @stop_after && rows.size + @total_deleted > @stop_after

      rows = rows.first(@stop_after - @total_deleted) if stopped
      if copy_mode?
        metric_op_name = 'INSERT'
        statement = <<-EOF
            insert into #{@target_model.table_name} (#{@target_keys.join(",")})
            values #{rows.map{|vec| "(#{vec.join(",")})"}.join(",")}
        EOF
      else
        metric_op_name = 'DELETE'
        statement = "delete from #{@target_model.table_name} #{build_target_where(rows)}"
      end
      if @dry_run && !copy_mode?
        @total_deleted += rows.size
        log :debug, "NOT DOING: #{statement}" if @logger.level == Logger::DEBUG
      else
        log :debug, statement if @logger.level == Logger::DEBUG
        chunk_deleted = NewRelic::Agent.with_database_metric_name(@target_model, metric_op_name) do
          @target_model.connection.update statement
        end

        @total_deleted += chunk_deleted
      end
      raise CleanSweep::PurgeStopped.new("stopped after #{verb} #{@total_deleted} #{@target_model} records", @total_deleted) if stopped
      q = @query.where(@index_entrypoint, index_entrypoint_args).to_sql
      log :debug, "find rows: #{q}" if @logger.level == Logger::DEBUG

      sleep @sleep if @sleep && !copy_mode?
      @mysql_status.check! if @mysql_status

      rows = NewRelic::Agent.with_database_metric_name(@source_model, 'SELECT') do
        @source_model.connection.select_rows(q)
      end
      report
    end
    report(true)
    if copy_mode?
      log :info,  "completed after copying #{@total_deleted} #{@source_model.table_name} records to #{@target_model.table_name}"
    else
      log :info,  "completed after deleting #{@total_deleted} records from #{@target_model.table_name}"
    end

    return @total_deleted
  end

  def sleep duration
    Kernel.sleep duration
  end

  add_method_tracer :sleep
  add_method_tracer :execute_in_batches

  private

  def build_target_where(rows)
    rec_criteria = rows.map do | row |
      row_compares = []
      @target_keys.each_with_index do | key, i |
        row_compares << "#{key}=#{row[i]}"
      end
      "(" + row_compares.join(" and ") + ")"
    end
    "where #{rec_criteria.join(" or ")}"
  end

end

