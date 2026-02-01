require "test_helper"
require "date"

class TestPathBuilder < Minitest::Test
  def test_s3_key_for_day_aggs
    key = Massive::FlatFiles::PathBuilder.s3_key(
      date: Date.new(2024, 10, 15),
      asset_class: :stocks,
      data_type: :day_aggs
    )

    assert_equal "us_stocks_sip/day_aggs_v1/2024/10/2024-10-15.csv.gz", key
  end

  def test_s3_key_accepts_string_date
    key = Massive::FlatFiles::PathBuilder.s3_key(
      date: "2024-10-15",
      asset_class: :stocks,
      data_type: :day_aggs
    )

    assert_equal "us_stocks_sip/day_aggs_v1/2024/10/2024-10-15.csv.gz", key
  end

  def test_s3_key_defaults_to_stocks_day_aggs
    key = Massive::FlatFiles::PathBuilder.s3_key(date: Date.new(2024, 10, 15))

    assert_equal "us_stocks_sip/day_aggs_v1/2024/10/2024-10-15.csv.gz", key
  end

  def test_s3_key_with_single_digit_month
    key = Massive::FlatFiles::PathBuilder.s3_key(date: Date.new(2024, 1, 5))

    assert_equal "us_stocks_sip/day_aggs_v1/2024/01/2024-01-05.csv.gz", key
  end

  def test_local_path_for_day_aggs
    path = Massive::FlatFiles::PathBuilder.local_path(
      date: Date.new(2024, 10, 15),
      asset_class: :stocks,
      data_type: :day_aggs,
      base_dir: "/tmp/test"
    )

    assert_equal "/tmp/test/us_stocks_sip/day_aggs_v1/2024/10/2024-10-15.csv.gz", path
  end

  def test_local_path_requires_base_dir
    error = assert_raises(ArgumentError) do
      Massive::FlatFiles::PathBuilder.local_path(date: Date.new(2024, 10, 15))
    end

    assert_match(/base_dir/, error.message)
  end

  def test_local_path_expands_tilde
    path = Massive::FlatFiles::PathBuilder.local_path(
      date: Date.new(2024, 10, 15),
      base_dir: "~/custom"
    )

    assert_equal "#{Dir.home}/custom/us_stocks_sip/day_aggs_v1/2024/10/2024-10-15.csv.gz", path
  end
end
