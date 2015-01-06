ENV['RACK_ENV'] = 'test'

require "codeclimate-test-reporter"
CodeClimate::TestReporter.start
require 'clean_sweep'
require 'factory_girl'
require 'fileutils'
require 'active_record'
require 'mysql2'
RSpec.configure do |config|
  config.include FactoryGirl::Syntax::Methods
  config.formatter = :progress
  #config.order = 'random'

  config.before(:suite) do
    FactoryGirl.find_definitions
  end

end

logdir = File.expand_path "../../log",__FILE__
FileUtils.mkdir_p logdir
logfile = File.open(File.join(logdir, "test.log"), "w+")
ActiveRecord::Base.logger = Logger.new(logfile)

database = { 
  encoding: 'utf8',
  adapter: 'mysql2',
  username: ENV['DB_USERNAME'] || 'root',
  host: 'localhost',
  password: ENV['DB_PASSWORD'],
}
db_name = ENV['DB_SCHEMA'] || 'cstest'
connection = Mysql2::Client.new(database)
connection.query "CREATE DATABASE IF NOT EXISTS #{db_name}"
database[:database] = db_name

ActiveRecord::Base.establish_connection(database)
