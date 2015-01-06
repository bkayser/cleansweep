Cleansweep is a utility for scripting purges using ruby in an
efficient, low-impact manner on mysql innodb tables.  Based on the
Percona `pt-archive` utility.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'cleansweep'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install cleansweep

## How it works

Consider the table:
```sql
    create table comments (
       `id` int(11) primary key auto_increment,
       `timestamp` datetime,
       `account` int(11),
       `liked` boolean,
       key comments_on_account_timestamp(account, timestamp)
    )
```
Assume there is an active record model for it:

    class Comment < ActiveRecord::Base ; end

### Purging by traversing an index

The most efficient way to work through a table is by scanning through
an index one chunk at a time.

Let's assume we want to purge Comments older than 1 month.  We can
scan the primary key index or the `account`,`timestamp` index.  In
this case the latter will probably work better since we are evaluating
the timestamp for the purge.

```ruby
    r = CleanSweep::PurgeRunner.new model: Comment,
                                    index: 'comments_on_account_timestamp' do | scope |
        scope.where('timestamp < ?', 1.month.ago)
    end
```

To execute the purge, do:

```ruby
    count = r.execute_in_batches
    puts "Deleted #{count} rows"
```

Check what it will do:

```ruby
    r.print_queries($stdout)
```

This will show you what it will do by printing out the three different
statements used:

```sql
    Initial Query:
        SELECT  `id`,`account`,`timestamp`
        FROM `comments` FORCE INDEX(comments_on_account_timestamp)
        WHERE (timestamp < '2014-11-25 21:47:43')
        ORDER BY `account` ASC,`timestamp` ASC
        LIMIT 500
    Chunk Query:
        SELECT  `id`,`account`,`timestamp`
        FROM `comments` FORCE INDEX(comments_on_account_timestamp)
        WHERE (timestamp < '2014-11-25 21:47:43') AND (`account` > 0 OR (`account` = 0 AND `timestamp` > '2014-11-18 21:47:43'))
    ORDER BY `account` ASC,`timestamp` ASC
        LIMIT 500
    Delete Statement:
        DELETE
        FROM `comments`
        WHERE (`id` = 2)
```

It does the initial statement once to get the first chunk of rows.
Then it does subsequent queries starting at the index where the last
chunk left off, thereby avoiding a complete index scan.  This works
fine as long as you don't have rows with duplicate account id and
timestamps.  If you do, you'll possibly miss rows between chunks.

To avoid missing duplicates, you can traverse the index using only the
first column with an inclusive comparator like `>=` instead of `>`.
Here's what that would look like:

```ruby
    r = CleanSweep::PurgeRunner.new model:Comment,
                                    index: 'comments_on_account_timestamp',
                                    first_only: true do | scope |
        scope.where('timestamp < ?', 1.month.ago)
    end
```

The chunk query looks like:

```sql
    SELECT  `id`,`account`,`timestamp`
    FROM `comments` FORCE INDEX(comments_on_account_timestamp)
    WHERE (timestamp < '2014-11-25 21:47:43') AND (`account` >= 0)
    LIMIT 500
```

You can scan the index in either direction.  To specify descending
order, use the `reverse: true` option.

If no index is specified, it will pick the primary key or the first unique index if there
is no primary key.

### Copying rows from one table to another

You can use the same technique to copy rows from one table to another.
Support in CleanSweep is pretty minimal.  It won't _move_ rows, only
copy them, although it would be easy to fix this.  I used this to copy
ids into a temporary table which I then used to delete later.

Here's an example that copies rows from the `Comment` model to the
`ExpiredComment` model (`expired_comments`).  Comments older than one
week are copied.

```ruby
      copier = CleanSweep::PurgeRunner.new model: Comment,
                                           index: 'comments_on_account_timestamp',
                                           dest_model: ExpiredComment,
                                           copy_only: true,
                                           copy_columns: %w[liked] do do | model |
        model.where('last_used_at < ?', 1.week.ago)
      end
```

The `copy_columns` option specifies additional columns to be inserted
into the `expired_comments` table.

If the column names are different in the destination table than in the
source table, you can specify a mapping with the `dest_columns` option
which takes a map of source column name to destination name.

### Deleting rows in another table

What if you want to query one table and delete those rows in another?
I needed this when I built a temporary table of account ids that
referenced deleted accounts.  I then wanted to delete rows in other
tables that referenced those account ids.  To do that, specify a
`dest_table` without specifying `copy_only` mode.  This will execute
the delete statement on the destination table without removing rows
from the source table.

Here's an example:

```sql
      create temporary table expired_metrics (
           metric_id int,
           account_id int,
           primary key (account_id, metric_id)
      EOF
```
Then run a job to pull account_id, metric_id into the expired metrics table:

```ruby
copier = CleanSweep::PurgeRunner.new index: 'index_on_metric_account_id',
                                     model: AccountMetric,
                                     dest_model: ExpiredMetric,
                                     copy_only: true) do | model |
    model.where("last_used_at < ?)", expiration_date)
end
copier.execute_in_batches
```

Now create as many jobs as you need for the tables which refer to these metrics:

```ruby
CleanSweep::PurgeRunner.new(model: ExpiredMetric,
                            dest_model: Metric,
                            dest_columns: { 'metric_id' => 'id'} ).execute_in_batches

CleanSweep::PurgeRunner.new(model: ExpiredMetric,
                            dest_model: ChartMetric).execute_in_batches

CleanSweep::PurgeRunner.new(model: ExpiredMetric,
                            dest_model: SystemMetric).execute_in_batches
```

These will delete the expired metrics from all the tables that refer to them.

### Watching the history list and replication lag

You can enter thresholds for the history list size and replication lag
that will be used to pause the purge if either of those values get
into an unsafe territory.  The script will pause for 5 minutes and
only start once the corresponding metric goes back down to 90% of the
specified threshold.

Note: You will need process privileges to be able to see the history list and
replication client privileges to monitor the replication lag.


### Logging and monitoring progress

You pass in a standard log instance to capture all running output.  By
default it will log to your `ActiveRecord::Base` logger, or stdout if
that's not set up.

If you specify a reporting interval with the `report` option it will
print the status of the purge at that interval.  This is useful to
track progress and assess the rate of deletion.

### Joins and subqueries

You can add subqueries and joins to your query in the scope block, but
be careful.  The index and order clause may work against you if the
table you are joining with doesn't have good parity with the indexes
in your target table.

### Limitations

* Only works for mysql.  I have only used it against 5.5.
* Should work with ActiveRecord 3.* - 4.*.  The tests only work on 4.*.
* Using a non-unique index risks missing duplicate rows unless you use the `first_only` option.
* Using the `first_only` option risks rescanning many rows if you have many more duplicates than your
  chunk size
* An index is required but you should be able to run a purge without one.  It just means you're not
  scanning the index in chunks.  This might be okay if you are deleting everything as you go along because
  then you're not rescanning the rows.  It wouldn't require much to modify CleanSweep to support this
  mode.

### Other options

There are a number of other options you can use to tune the script.
For details look at the [API on the `PurgeRunner`
class](http://bkayser.github.io/cleansweep/rdoc/CleanSweep/PurgeRunner.html)

### New Relic integration

The script requires the [New Relic](http://github.com/newrelic/rpm)
gem.  It won't impact anyting if you don't have a New Relic account to
report to, but if you do use New Relic it is configured to show you
detailed metrics.

In order to see the data in New Relic your purge must be identified as
a background transaction.  If you are running in Resque or DelayedJob,
it will automatically be tagged as such, but if you are just invoking
your purge directly, you'll need to tag it as a background
transaction.  The easy way to do that is shown in this example:

```ruby
    class Purge
      include NewRelic::Agent::Instrumentation::ControllerInstrumentation
      def run()
         ...
      end
      add_transaction_tracer :run
    end
```
Also, I recommend turning off transaction traces for long
purge jobs to reduce your memory footprint.

## Testing

To run the specs, start a local mysql instance.  The default user is
root with an empty password.  Override the user/password with
environment variables `DB_USER` and `DB_PASSWORD`.  The test creates a
db called 'cstest'.

## Contributing

1. Fork it ( https://github.com/bkayser/cleansweep/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## License and Copyright

Copyright 2014 New Relic, Inc., and Bill Kayser

Covered by the MIT [LICENSE](LICENSE.txt).

### Credits

This was all inspired and informed by [Percona's `pt-archiver`
script](http://www.percona.com/doc/percona-toolkit/2.1/pt-archiver.html)
written by Baron Schwartz.
