
class CleanSweep::PurgeStopped < RuntimeError

  attr_reader :stopped_at

  def initialize(message, limit)
    @stopped_at = limit
  end
end