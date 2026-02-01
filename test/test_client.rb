require "test_helper"
require "date"
require "fileutils"
require "tmpdir"
require "zlib"
require "stringio"

class TestClient < Minitest::Test
  def setup
    @tmpdir = Dir.mktmpdir
    @client = Massive::FlatFiles::Client.new(
      access_key_id: "test_key",
      secret_access_key: "test_secret",
    )
  end

  def teardown
    FileUtils.rm_rf(@tmpdir) if @tmpdir
  end

  def test_client_initializes_with_credentials
    client = Massive::FlatFiles::Client.new(
      access_key_id: "test_key",
      secret_access_key: "test_secret",
    )

    assert_instance_of Massive::FlatFiles::Client, client
  end

  def test_client_requires_access_key_id
    error = assert_raises(ArgumentError) do
      Massive::FlatFiles::Client.new(secret_access_key: "test")
    end

    assert_match(/access_key_id/, error.message)
  end

  def test_client_requires_secret_access_key
    error = assert_raises(ArgumentError) do
      Massive::FlatFiles::Client.new(access_key_id: "test")
    end

    assert_match(/secret_access_key/, error.message)
  end

  def test_download_file
    date = Date.new(2024, 10, 1)
    local_path = File.join(@tmpdir, "test.csv.gz")

    mock_s3_client = Minitest::Mock.new
    mock_s3_client.expect(:get_object, nil) do |params|
      params[:bucket] == "flatfiles" &&
      params[:key] == "us_stocks_sip/day_aggs_v1/2024/10/2024-10-01.csv.gz" &&
      params[:response_target] == local_path
    end

    @client.stub(:s3_client, mock_s3_client) do
      FileUtils.mkdir_p(File.dirname(local_path))
      File.write(local_path, "test data")

      result = @client.download_file(date: date, local_path: local_path)
      assert_equal local_path, result
    end

    mock_s3_client.verify
  end

  def test_file_exists_returns_true_when_file_exists
    date = Date.new(2024, 10, 1)

    mock_response = Minitest::Mock.new
    mock_s3_client = Minitest::Mock.new
    mock_s3_client.expect(:head_object, mock_response) do |params|
      params[:bucket] == "flatfiles" &&
      params[:key] == "us_stocks_sip/day_aggs_v1/2024/10/2024-10-01.csv.gz"
    end

    @client.stub(:s3_client, mock_s3_client) do
      assert @client.file_exists?(date: date)
    end

    mock_s3_client.verify
  end

  def test_file_exists_returns_false_when_file_not_found
    date = Date.new(2024, 10, 1)

    mock_s3_client = Minitest::Mock.new
    mock_s3_client.expect(:head_object, nil) do |params|
      raise Aws::S3::Errors::NotFound.new(nil, "Not Found")
    end

    @client.stub(:s3_client, mock_s3_client) do
      refute @client.file_exists?(date: date)
    end

    mock_s3_client.verify
  end

  def test_file_metadata
    date = Date.new(2024, 10, 1)

    mock_response = Object.new
    def mock_response.content_length; 204800; end
    def mock_response.last_modified; Time.new(2024, 10, 2); end
    def mock_response.etag; "\"abc123\""; end

    mock_s3_client = Minitest::Mock.new
    mock_s3_client.expect(:head_object, mock_response) do |params|
      params[:bucket] == "flatfiles" &&
      params[:key] == "us_stocks_sip/day_aggs_v1/2024/10/2024-10-01.csv.gz"
    end

    @client.stub(:s3_client, mock_s3_client) do
      metadata = @client.file_metadata(date: date)

      assert_equal 204800, metadata[:size]
      assert_equal Time.new(2024, 10, 2), metadata[:last_modified]
      assert_equal "\"abc123\"", metadata[:etag]
    end

    mock_s3_client.verify
  end

  def test_stream_file
    date = Date.new(2024, 10, 1)

    csv_data = "ticker,volume,open,close,high,low,window_start,transactions\nAAPL,100,150.0,151.0,152.0,149.0,1727755200000000000,1000\n"

    gz_string = StringIO.new
    gz = Zlib::GzipWriter.new(gz_string)
    gz.write(csv_data)
    gz.close
    gz_data = gz_string.string

    mock_body = Object.new
    def mock_body.read; @data; end
    mock_body.instance_variable_set(:@data, gz_data)

    mock_response = Object.new
    def mock_response.body; @body; end
    mock_response.instance_variable_set(:@body, mock_body)

    mock_s3_client = Minitest::Mock.new
    mock_s3_client.expect(:get_object, mock_response) do |params|
      params[:bucket] == "flatfiles" &&
      params[:key] == "us_stocks_sip/day_aggs_v1/2024/10/2024-10-01.csv.gz"
    end

    @client.stub(:s3_client, mock_s3_client) do
      result = @client.stream_file(date: date)
      assert_includes result, "AAPL"
      assert_includes result, "ticker,volume"
    end

    mock_s3_client.verify
  end

  def test_list_dates
    mock_contents = [
      Object.new.tap { |o| def o.key; "us_stocks_sip/day_aggs_v1/2024/10/2024-10-01.csv.gz"; end },
      Object.new.tap { |o| def o.key; "us_stocks_sip/day_aggs_v1/2024/10/2024-10-02.csv.gz"; end },
      Object.new.tap { |o| def o.key; "us_stocks_sip/day_aggs_v1/2024/10/2024-10-03.csv.gz"; end },
    ]

    mock_response = Object.new
    def mock_response.contents; @contents; end
    def mock_response.is_truncated; false; end
    mock_response.instance_variable_set(:@contents, mock_contents)

    mock_s3_client = Minitest::Mock.new
    mock_s3_client.expect(:list_objects_v2, mock_response) do |params|
      params[:bucket] == "flatfiles" &&
      params[:prefix].start_with?("us_stocks_sip/day_aggs_v1/2024/10")
    end

    @client.stub(:s3_client, mock_s3_client) do
      dates = @client.list_dates(year: 2024, month: 10)

      assert_equal 3, dates.size
      assert_equal Date.new(2024, 10, 1), dates[0]
      assert_equal Date.new(2024, 10, 2), dates[1]
      assert_equal Date.new(2024, 10, 3), dates[2]
    end

    mock_s3_client.verify
  end
end
