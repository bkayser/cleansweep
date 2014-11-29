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



## Contributing

1. Fork it ( https://github.com/[my-github-username]/cleansweep/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
