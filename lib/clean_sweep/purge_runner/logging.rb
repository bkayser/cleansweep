module CleanSweep::PurgeRunner::Logging

  def report(force=false)
    report_duration = Time.now - @report_interval_start
    if (force || report_duration >= @report_interval)
      while (@report_interval > 0 && @report_interval_start < Time.now - @report_interval) do
        @report_interval_start += @report_interval
      end
      print_report
    end
  end

  def log level, msg
    prefix = level == :debug ? " *** " : " ** "
    out = msg.split("\n").map {|line| prefix + line}.join("\n")
    @logger.send level, out
  end

  def print_queries
    io = StringIO.new
    io.puts 'Initial Query:'
    io.puts format_query('    ', @query.to_sql)
    io.puts "Chunk Query:"
    io.puts format_query('    ', @table_schema.scope_to_next_chunk(@query, sample_rows.first).to_sql)
    if copy_mode?
      io.puts "Insert Statement:"
      io.puts format_query('    ', @table_schema.insert_statement(sample_rows))
    else
      io.puts "Delete Statement:"
      io.puts format_query('    ', @table_schema.delete_statement(sample_rows))
    end
    io.string
  end

  private 

  def sample_rows
    @sample_rows ||= @model.connection.select_rows @query.limit(1).to_sql
    if @sample_rows.empty?
      # Don't have any sample data to use for the sample queries, so use NULL values just
      # so the query will print out.
      @sample_rows << [nil] * 100
    end
    @sample_rows
  end

  def format(time)
    format_string = "%H:%M:%S"
    if (time.to_i > (24 * 60 * 60))
      format_string = "%d days, %H:%M"
    end
    Time.at(time).utc.strftime(format_string)
  end

  def print_report
    elapsed = [1, (Time.now - @start).to_i].max
    rate    = (@total_deleted / elapsed).to_i
    rate    = "#{rate > 0 ? '%12i' % rate : ('%12s' %'< 1')} records/second"
    report = [ "report:" ]
    action = case
             when @dry_run then 'queried'
             when copy_mode? then 'copied'
             else 'deleted'
             end
    report << "  #{action}: #{'%12i' % @total_deleted} #{@model.table_name} records"
    report << "  elapsed: #{'%12s' % format(elapsed)}"
    report << "  rate:    #{rate}"
    log :info,  report.join("\n")
  end
end
