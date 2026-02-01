# Massive::FlatFiles

Ruby client for accessing flat files from [Massive.com](https://massive.com).

## Features

- **Simple API**: Download and read flat files with minimal code
- **Auto-authentication**: Automatically uses credentials from `massive-account` gem
- **Manual credentials**: Override with explicit S3 credentials when needed
- **CSV parsing**: Automatic decompression and parsing of gzipped CSV files
- **Local caching**: Sync files to local storage for fast repeated access
- **Live streaming**: Fetch and process data without saving to disk

## Installation

Add to your Gemfile:

```ruby
gem 'massive-flat_files'
gem 'massive-account'  # For automatic credential detection
```

Or install directly:

```bash
gem install massive-flat_files
```

## Quick Start

```ruby
require 'massive/flat_files'

# Set local directory for downloaded files
ENV['MASSIVE_FLAT_FILES_DIR'] = '/data/flat_files'

# Sync a file from S3 (auto-detects credentials from massive-account)
Massive::FlatFiles.sync(date: '2024-10-01')

# Read the synced file
rows = Massive::FlatFiles.read(date: '2024-10-01')

# Access data
rows.each do |row|
  puts "#{row[:ticker]}: $#{row[:close]}"
end

# Find specific ticker
aapl = rows.find { |r| r[:ticker] == 'AAPL' }
puts "AAPL closed at $#{aapl[:close]} with volume #{aapl[:volume]}"
```

## Usage

### Syncing Files

Download files from S3 to local storage. You must specify where to store files either via:
1. `MASSIVE_FLAT_FILES_DIR` environment variable (recommended)
2. `local_dir:` parameter

```ruby
# Option 1: Set environment variable (recommended)
ENV['MASSIVE_FLAT_FILES_DIR'] = '/data/flat_files'
Massive::FlatFiles.sync(date: '2024-10-01')

# Option 2: Pass local_dir explicitly
Massive::FlatFiles.sync(date: '2024-10-01', local_dir: '/custom/path')

# With manual credentials
Massive::FlatFiles.sync(
  date: '2024-10-01',
  local_dir: '/data/flat_files',
  access_key_id: 'your_key',
  secret_access_key: 'your_secret'
)
```

If neither is provided, a `ConfigurationError` will be raised.

### Reading Synced Files

Read files that have been synced locally:

```ruby
rows = Massive::FlatFiles.read(date: '2024-10-01')

# Returns array of hashes with typed fields:
# {
#   ticker: "AAPL",           # String
#   volume: 63285048,         # Integer
#   open: 229.52,             # Float
#   close: 226.21,            # Float
#   high: 229.65,             # Float
#   low: 223.74,              # Float
#   window_start: 1727755200000000000,  # Integer (nanoseconds)
#   transactions: 832120      # Integer
# }
```

### Fetching Without Saving

Stream data directly from S3 without local storage:

```ruby
# Process all rows
Massive::FlatFiles.fetch(date: '2024-10-01') do |row|
  puts "#{row[:ticker]}: $#{row[:close]}"
end

# Or get all rows as array
rows = Massive::FlatFiles.fetch(date: '2024-10-01')
```

### Listing Available Files

```ruby
# List all dates available for a month
dates = Massive::FlatFiles.list_remote(year: 2024, month: 10)
# => [Date(2024-10-01), Date(2024-10-02), ...]

# List entire year
dates = Massive::FlatFiles.list_remote(year: 2024)
```

### Checking File Existence

```ruby
if Massive::FlatFiles.file_exists?(date: '2024-10-01')
  puts "File is available"
end
```

### Getting Local File Path

```ruby
ENV['MASSIVE_FLAT_FILES_DIR'] = '/data/flat_files'
path = Massive::FlatFiles.local_path(date: '2024-10-01')
# => "/data/flat_files/us_stocks_sip/day_aggs_v1/2024/10/2024-10-01.csv.gz"
```

## Authentication

### Auto-detection (Recommended)

By default, credentials are automatically detected from `massive-account`:

```ruby
# Set environment variables
ENV['MASSIVE_ACCOUNT_EMAIL'] = 'your@email.com'
ENV['MASSIVE_ACCOUNT_PASSWORD'] = 'your_password'

# Credentials auto-detected
Massive::FlatFiles.sync(date: '2024-10-01')
```

### Manual Credentials

Override with explicit S3 credentials:

```ruby
Massive::FlatFiles.sync(
  date: '2024-10-01',
  access_key_id: 'your_s3_key',
  secret_access_key: 'your_s3_secret'
)
```

All methods that access S3 accept these optional parameters:
- `access_key_id:`
- `secret_access_key:`

## Data Format

### US Stocks - Day Aggregates

The default (and currently only accessible) data type is US stock daily aggregates.

**CSV Schema:**
- `ticker` (String): Stock symbol
- `volume` (Integer): Total shares traded
- `open` (Float): Opening price
- `close` (Float): Closing price
- `high` (Float): Highest price
- `low` (Float): Lowest price
- `window_start` (Integer): Unix timestamp in nanoseconds
- `transactions` (Integer): Number of trades

**Example:**
```
ticker,volume,open,close,high,low,window_start,transactions
AAPL,63285048,229.52,226.21,229.65,223.74,1727755200000000000,832120
```

## Subscription Tiers

Access to flat files depends on your Massive.com subscription tier:

- **Tier**: Determines which asset classes and data types you can access
- **Historical Data**: Limited by `historical_years` in your subscription
  - Example: "starter" tier = 5 years of historical data (2021 onwards as of 2025)

Check your access:
```ruby
require 'massive/account'

account = Massive::Account.info
tier = account.dig(:resources, :stocks, :tier)
historical_years = account.dig(:resources, :stocks, :historical_years)
has_flat_files = account.dig(:resources, :stocks, :features, :flat_files)

puts "Tier: #{tier}"
puts "Historical data: #{historical_years} years"
puts "Flat files enabled: #{has_flat_files}"
```

## Error Handling

```ruby
begin
  Massive::FlatFiles.sync(date: '2020-01-01')  # Outside historical range
rescue Massive::FlatFiles::PermissionError => e
  puts "Access denied: #{e.message}"
rescue Massive::FlatFiles::FileNotFoundError => e
  puts "File not found: #{e.message}"
rescue Massive::FlatFiles::CredentialError => e
  puts "Credential error: #{e.message}"
rescue Massive::FlatFiles::Error => e
  puts "Error: #{e.message}"
end
```

### Error Types

- `Massive::FlatFiles::Error` - Base error class
- `Massive::FlatFiles::PermissionError` - Access denied (wrong tier or outside historical range)
- `Massive::FlatFiles::HistoricalDataError` - Date outside your `historical_years` limit
- `Massive::FlatFiles::FileNotFoundError` - File doesn't exist
- `Massive::FlatFiles::CredentialError` - Invalid or missing credentials

# Run tests with live credentials
ruby dev/run_tests_with_creds.rb

# Run demo
ruby dev/demo.rb
```

## Testing

The gem includes both unit tests and integration tests:

```bash
# Run all tests (integration tests skipped without credentials)
bundle exec rake test

# Run with live S3 access (uses massive-account credentials)
ruby dev/run_tests_with_creds.rb
```

## Links

- [Massive.com](https://massive.com) - Market data provider
- [Flat Files Documentation](https://massive.com/docs/flat-files/stocks/overview)
- [massive-account gem](https://github.com/Vadrigar/massive-account) - Credential management

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/v7-data/massive-flat_files.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
