module CleanSweep::PurgeRunner::Logging

  def report(force=false)
    report_duration = Time.now - @report_interval_start
    if (force || report_duration >= @report_interval)
      while (@report_interval_start < Time.now - @report_interval) do
        @report_interval_start += @report_interval
      end
      report  = []
      elapsed = [1, (Time.now - @start).to_i].max
      rate    = (@total_deleted / elapsed).to_i
      rate    = "#{rate > 0 ? '%12i' % rate : ('%12s' %'< 1')} records/second"
      report << "report:"
      if copy_mode?
        report << "  copied #{'%12i' % @total_deleted} #{@source_model.table_name} records"
      else
        report << "  #{@dry_run ? 'queried' : 'deleted'}: #{'%12i' % @total_deleted} #{@target_model.table_name} records"
      end
      report << "  elapsed: #{'%12s' % format(elapsed)}"
      report << "  rate:    #{rate}"
      log :info,  report.join("\n")
    end
  end

  def log level, msg
    prefix = level == :debug ? " *** " : " ** "
    out = msg.split("\n").map {|line| prefix + line}.join("\n")
    @logger.send level, out
  end

  def format(time)
    format_string = "%H:%M:%S"
    if (time.to_i > (24 * 60 * 60))
      format_string = "%d days, %H:%M"
    end
    Time.at(time).strftime(format_string)
  end
end