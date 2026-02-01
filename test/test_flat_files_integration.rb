require "test_helper"
require "date"
require "fileutils"
require "tmpdir"
require "csv"
require "zlib"
require "stringio"
require "massive/account"

class TestFlatFilesIntegration < Minitest::Test
  def setup
    @tmpdir = Dir.mktmpdir
    ENV["MASSIVE_FLAT_FILES_DIR"] = @tmpdir

    @mock_credentials = {
      credential_sets: [
        {
          s3: {
            access_key_id: "test_key",
            secret_access_key: "test_secret",
          },
        },
      ],
    }
  end

  def teardown
    FileUtils.rm_rf(@tmpdir) if @tmpdir
    ENV.delete("MASSIVE_FLAT_FILES_DIR")
  end

  def test_sync_requires_local_dir_or_env
    ENV.delete("MASSIVE_FLAT_FILES_DIR")

    error = assert_raises(Massive::FlatFiles::ConfigurationError) do
      Massive::FlatFiles.sync(
        date: Date.new(2024, 10, 1),
        access_key_id: "test",
        secret_access_key: "test",
      )
    end

    assert_match(/MASSIVE_FLAT_FILES_DIR/, error.message)
  end

  def test_sync_with_auto_credentials
    date = Date.new(2024, 10, 1)
    expected_path = File.join(@tmpdir, "us_stocks_sip", "day_aggs_v1", "2024", "10", "2024-10-01.csv.gz")

    with_stubbed_account_info(@mock_credentials) do
      mock_client = Object.new
      mock_client.define_singleton_method(:download_file) do |**params|
        FileUtils.mkdir_p File.dirname(params[:local_path])
        File.write params[:local_path], "mock data"
        params[:local_path]
      end

      Massive::FlatFiles::Client.stub :new, ->(**_args) { mock_client } do
        local_path = Massive::FlatFiles.sync(date: date)

        assert File.exist?(local_path), "File should be downloaded"
        assert_equal expected_path, local_path
        assert local_path.end_with?("2024-10-01.csv.gz"), "File should be named correctly"
      end
    end
  end

  def test_sync_with_manual_credentials
    date = Date.new(2024, 10, 1)

    mock_client = Object.new
    mock_client.define_singleton_method(:download_file) do |**params|
      FileUtils.mkdir_p File.dirname(params[:local_path])
      File.write params[:local_path], "mock data"
      params[:local_path]
    end

    Massive::FlatFiles::Client.stub :new, ->(**_args) { mock_client } do
      local_path = Massive::FlatFiles.sync(
        date: date,
        access_key_id: "manual_key",
        secret_access_key: "manual_secret",
      )

      assert File.exist?(local_path)
    end
  end

  def test_read_file
    date = Date.new(2024, 10, 1)

    csv_data = "ticker,volume,open,close,high,low,window_start,transactions\nAAPL,100,150.0,151.0,152.0,149.0,1727755200000000000,1000\n"
    local_path = Massive::FlatFiles.local_path(date: date)

    FileUtils.mkdir_p File.dirname(local_path)
    File.open(local_path, "wb") do |f|
      gz = Zlib::GzipWriter.new(f)
      gz.write csv_data
      gz.close
    end

    rows = Massive::FlatFiles.read(date: date)

    assert rows.is_a?(Array), "Should return array"
    assert_equal 1, rows.size
    assert rows.first.is_a?(Hash), "Rows should be hashes"
    assert_equal "AAPL", rows.first[:ticker]
    assert_equal 151.0, rows.first[:close]
  end

  def test_fetch_without_saving
    date = Date.new(2024, 10, 1)

    with_stubbed_account_info(@mock_credentials) do
      mock_client = Object.new
      mock_client.define_singleton_method(:stream_file) do |**_params|
        csv_data = "ticker,volume,open,close,high,low,window_start,transactions\n"
        10.times { |i| csv_data += "TICK#{i},100,150.0,151.0,152.0,149.0,1727755200000000000,1000\n" }
        csv_data
      end

      Massive::FlatFiles::Client.stub :new, ->(**_args) { mock_client } do
        rows = []
        Massive::FlatFiles.fetch(date: date) do |row|
          rows << row
          break if rows.size >= 10
        end

        assert_equal 10, rows.size
        assert rows.first.key?(:ticker)
      end
    end
  end

  def test_list_remote
    with_stubbed_account_info(@mock_credentials) do
      mock_client = Object.new
      mock_client.define_singleton_method(:list_dates) do |**_params|
        [Date.new(2024, 10, 1), Date.new(2024, 10, 2), Date.new(2024, 10, 3)]
      end

      Massive::FlatFiles::Client.stub :new, ->(**_args) { mock_client } do
        dates = Massive::FlatFiles.list_remote(year: 2024, month: 10)

        assert dates.is_a?(Array)
        assert_equal 3, dates.size
        assert dates.all? { |d| d.is_a?(Date) }
        assert dates.all? { |d| d.year == 2024 && d.month == 10 }
      end
    end
  end

  def test_file_exists
    call_count = 0

    with_stubbed_account_info(@mock_credentials) do
      mock_client = Object.new
      mock_client.define_singleton_method(:file_exists?) do |**_params|
        call_count += 1
        call_count == 1
      end

      Massive::FlatFiles::Client.stub :new, ->(**_args) { mock_client } do
        assert Massive::FlatFiles.file_exists?(date: Date.new(2024, 10, 1))
        refute Massive::FlatFiles.file_exists?(date: Date.today + 365)
      end
    end
  end

  private

  def with_stubbed_account_info(return_value)
    original = Massive::Account.method(:info)
    silence_warnings do
      Massive::Account.define_singleton_method(:info) { |**_args| return_value }
    end
    yield
  ensure
    if original
      silence_warnings do
        Massive::Account.singleton_class.define_method(:info, original)
      end
    end
  end

  def silence_warnings
    old_verbose = $VERBOSE
    $VERBOSE = nil
    yield
  ensure
    $VERBOSE = old_verbose
  end
end
