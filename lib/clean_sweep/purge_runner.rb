# This is a utility built to mimic some of the features of the pt_archive script
# to make really big purges go faster with no production impact.
#
# It uses a strategy of descending an index, querying for purgeable ids and then
# deleting them in batches.
#
# === Required Options
#
# [:model]
#    Required: the Active Record model for the table being purged or copied from
#
# === Optional Options
#
# [:chunk_size]
#    The number of rows to copy in each block.  Defaults to 500.
# [:index]
#    The index to traverse in ascending order doing the purge.  Rows are read in the order of
#    the index, which must be a btree index.  If not specified, <tt>PRIMARY</tt> is assumed.
# [:reverse]
#    Traverse the index in reverse order.  For example, if your index is on <tt>account_id</tt>,
#    <tt>timestamp</tt>, this option will move through the rows starting at the highest account
#    number, then move through timestamps starting with the most recent.
# [:dry_run]
#    Goes through the work of finding the rows but does not execute a delete or insert.
# [:stop_after]
#    The operation will end after copying this many rows.
# [:report]
#    Specify an interval in seconds between status messages being printed out.
# [:logger]
#    The log instance to use.  Defaults to the <tt>ActiveRecord::Base.logger</tt>
#    if not nil, otherwise it uses _$stdout_
# [:dest_model]
#    When this option is present nothing is deleted, and instead rows are copied to
#    the table for this model.  This model must
#    have identically named columns as the source model.  By default, only columns in the
#    named index and primary key are copied but these can be augmented with columns in the
#    <tt>copy_columns</tt> option.
# [:copy_columns]
#    Extra columns to add when copying to a dest model.
#
# === Safety thresholds
# [:sleep]
#    Time in seconds to sleep between each chunk.
# [:max_history]
#    The history list size (if available) is checked every 5 minutes and if it exceeds this size
#    the purge will pause until the history list is below 90% of this value.
# [:max_repl_lag]
#    The maximum length of the replication lag.  Checked every 5 minutes and if exceeded the purge
#    pauses until the replication lag is below 90% of this value.

#
class CleanSweep::PurgeRunner

  require 'clean_sweep/purge_runner/logging'
  require 'clean_sweep/purge_runner/mysql_status'

  include CleanSweep::PurgeRunner::Logging

  # This helps us track the state of replication and history list and pause
  # if necessary
  attr_reader :mysql_status
  
  def initialize(options={})
    @model     = options[:model] or raise "source model class required"
    @limit            = options[:chunk_size] || 500

    @target_model     = options[:dest_model]
    @stop_after       = options[:stop_after]
    @report_interval  = options[:report] || 10.seconds
    @logger           = options[:logger] || ActiveRecord::Base.logger || Logger.new($stdout)
    @dry_run          = options[:dry_run]
    @sleep            = options[:sleep]

    @max_history      = options[:max_history]
    @max_repl_lag     = options[:max_repl_lag]

    @table_schema     = CleanSweep::TableSchema.new @model,
                                                    key_name: options[:index],
                                                    ascending: !options[:reverse],
                                                    extra_columns: options[:copy_columns]

    if (@max_history || @max_repl_lag)
      @mysql_status = CleanSweep::PurgeRunner::MysqlStatus.new model: @model,
                                                               max_history: @max_history,
                                                               max_repl_lag: @max_repl_lag,
                                                               check_period: options[:check_period],
	                                                       logger: @logger
    end

    raise "You can't copy rows from a table into itself" if copy_mode? && @model == @target_model

    @report_interval_start = Time.now

    @query            = @table_schema.initial_scope.limit(@limit)

    @query = yield(@query) if block_given?
  end


  def copy_mode?
    @target_model.present?    
  end

  # Execute the purge in chunks according to the parameters given on instance creation.
  # Will raise <tt>CleanSweep::PurgeStopped</tt> if a <tt>stop_after</tt> option was provided and
  # that limit is hit.
  #
  # Returns the number of rows copied or deleted.
  #
  def execute_in_batches

    @start = Time.now
    verb = copy_mode? ? "copying" : "purging"
    verb = "NOT "+verb if @dry_run

    msg = "starting: #{verb} #{@table_schema.name} records in batches of #@limit"
    msg << " to #{@target_model.table_name}" if copy_mode?


    log :info,  "sleeping #{@sleep} seconds between purging" if @sleep && !copy_mode?
    @total_deleted = 0

    # Iterate through the rows in limit chunks
    log :debug, "find rows: #{@query.to_sql}" if @logger.level == Logger::DEBUG

    @mysql_status.check! if @mysql_status

    rows = NewRelic::Agent.with_database_metric_name(@model.name, 'SELECT') do
      @model.connection.select_rows @query.to_sql
    end
    while rows.any? && (!@stop_after || @total_deleted < @stop_after) do
#      index_entrypoint_args = Hash[*@source_keys.zip(rows.last).flatten]
      log :debug, "#{verb} #{rows.size} records between #{rows.first.inspect} and #{rows.last.inspect}" if @logger.level == Logger::DEBUG
      stopped = @stop_after && rows.size + @total_deleted > @stop_after

      rows = rows.first(@stop_after - @total_deleted) if stopped
      last_row = rows.last
      if copy_mode?
        metric_op_name = 'INSERT'
        statement = @table_schema.insert_statement(@target_model, rows)
      else
        metric_op_name = 'DELETE'
        statement = @table_schema.delete_statement(rows)
      end
      if @dry_run && !copy_mode?
        @total_deleted += rows.size
        log :debug, "NOT DOING: #{statement}" if @logger.level == Logger::DEBUG
      else
        log :debug, statement if @logger.level == Logger::DEBUG
        chunk_deleted = NewRelic::Agent.with_database_metric_name(@target_model, metric_op_name) do
          @model.connection.update statement
        end

        @total_deleted += chunk_deleted
      end
      raise CleanSweep::PurgeStopped.new("stopped after #{verb} #{@total_deleted} #{@model} records", @total_deleted) if stopped
      q = @table_schema.scope_to_next_chunk(@query, last_row).to_sql
      log :debug, "find rows: #{q}" if @logger.level == Logger::DEBUG

      sleep @sleep if @sleep && !copy_mode?
      @mysql_status.check! if @mysql_status

      rows = NewRelic::Agent.with_database_metric_name(@model, 'SELECT') do
        @model.connection.select_rows(q)
      end
      report
    end
    report(true)
    if copy_mode?
      log :info,  "completed after #{verb} #{@total_deleted} #{@table_schema.name} records to #{@target_model.table_name}"
    else
      log :info,  "completed after #{verb} #{@total_deleted} #{@table_schema.name} records"
    end

    return @total_deleted
  end

  def sleep duration
    Kernel.sleep duration
  end

  add_method_tracer :sleep
  add_method_tracer :execute_in_batches

  private



end

