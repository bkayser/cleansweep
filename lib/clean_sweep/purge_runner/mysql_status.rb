require 'newrelic_rpm'

class CleanSweep::PurgeRunner::MysqlStatus

  # Options:
  # logger, model, max_history, max_repl_lag, check_period
  def initialize(options={})
    @logger = options[:logger] || ActiveRecord::Base.logger
    @model = options[:model]
    @max_history = options[:max_history]
    @max_replication_lag = options[:max_repl_lag]
    @check_period = options[:check_period] || 2.minutes
    @last_check = @check_period.ago
  end

  def check!
    return if Time.now - @check_period < @last_check
    while (v = get_violations).any? do
      @logger.warn("pausing until threshold violations clear (#{v.to_a.map{ |key, value| "#{key} = #{value}"}.join(", ")})")
      @paused = true
      pause 5.minutes
    end
    @logger.info("violations clear") if paused?
    all_clear!
  end

  def get_violations
    violations = {}
    if @max_history
      current = get_history_length
      violations["history length"] = "#{(current/1_000_000.0)} m" if threshold(@max_history) < current
    end
    if @max_replication_lag
      current = get_replication_lag
      violations["replication lag"] = current if threshold(@max_replication_lag) < current
    end
    return violations
  end

  # Return the threshold to use in the check.  If we are already failing, don't
  # start up again until we've recovered at least 10%
  def threshold(value)
    if paused?
      value = 0.90 * value
    else
      value
    end
  end

  def pause time
    Kernel.sleep time
  end
  add_method_tracer :pause

  def paused?
    @paused
  end

  def all_clear!
    @last_check = Time.now
    @paused = nil
  end

  def get_replication_lag
    rows = @model.connection.select_rows 'SHOW SLAVE STATUS'
    if rows.nil? || rows.empty?
      return 0
    else
      return rows[0][32]
    end
  end


  def get_history_length
    rows = @model.connection.select_rows <<-EOF
        show engine innodb status
    EOF
    status_string = rows.first[2]

    # This output of 'show engine innnodb status' contains a bunch of
    # information, including info about the most recently detected deadlock,
    # which has the involved queries, which may contain invalid UTF-8 byte
    # sequences.
    #
    # Forcing the encoding to ASCII-8BIT here prevents the regex match below
    # from falling over when this happens.
    status_string.force_encoding('ASCII-8BIT')

    return /History list length ([0-9]+)/.match(status_string)[1].to_i
  end

end
