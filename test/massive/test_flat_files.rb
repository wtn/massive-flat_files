require "test_helper"

class Massive::TestFlatFiles < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::Massive::FlatFiles::VERSION
  end

  def test_constants_are_defined
    assert_equal "https://files.massive.com", Massive::FlatFiles::S3_ENDPOINT
    assert_equal "flatfiles", Massive::FlatFiles::S3_BUCKET
    assert_equal "us-east-1", Massive::FlatFiles::S3_REGION
  end
end
