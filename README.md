# Cleansweep

Utilities for doing purges in an efficient, low-impact manner on 
mysql innodb tables.  Loosely based on the Percona pt-archive utility.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'cleansweep'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install cleansweep

## Testing

To run the specs, start a local mysql instance.  The default user is root with an empty password.
Override the user/password with environment variables `DB_USER` and `DB_PASSWORD`.  The test
creates a db called 'cstest'.

## Examples

### Purging by a timestamp

Let's assume we want to purge Comments older than 1 month that have not been liked.  The best way
to move through the table wil be by `timestamp` so use the index on that column.  

(docs still a work in progress...)

### Copying rows from one table to another

Copy rows from `Metric` model (`metrics` table) to `ExpiredMetric` model (`expired_metrics`).
Metrics older than one week are copied.  Use the index on `account_id`, `metric_id`.  This only
makes sense if there's not an index on `last_used_at` because it's going to scan all the rows.

      expired_metrics_copier = CleanSweep::PurgeRunner.new \
           limit: 1000,
 	   copy: true,
           index: 'index_metrics_on_account_id_and_metric_id',
           keys: %w[account_id metric_id],
           target: ExpiredMetric,
           source: Metric) do | model |
        model.where('last_used_at < unix_timestamp(now() - INTERVAL 1 WEEK)')
      end
      copied_count = expired_metrics_copier.execute_in_batches


## Contributing

1. Fork it ( https://github.com/[my-github-username]/cleansweep/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
