require "aws-sdk-s3"
require "fileutils"
require "zlib"
require "stringio"

module Massive
  module FlatFiles
    class Client
      attr_reader :access_key_id, :secret_access_key

      def initialize(access_key_id:, secret_access_key:)
        raise ArgumentError, "access_key_id is required" if access_key_id.nil? || access_key_id.empty?
        raise ArgumentError, "secret_access_key is required" if secret_access_key.nil? || secret_access_key.empty?

        @access_key_id = access_key_id
        @secret_access_key = secret_access_key
      end

      def download_file(date:, local_path:, asset_class: :stocks, data_type: :day_aggs)
        s3_key = PathBuilder.s3_key(date: date, asset_class: asset_class, data_type: data_type)

        FileUtils.mkdir_p(File.dirname(local_path))

        s3_client.get_object(
          bucket: S3_BUCKET,
          key: s3_key,
          response_target: local_path,
        )

        local_path
      rescue Aws::S3::Errors::NoSuchKey
        raise FileNotFoundError, "File not found: #{s3_key}"
      rescue Aws::S3::Errors::Forbidden, Aws::S3::Errors::AccessDenied
        raise PermissionError, "Access denied to file: #{s3_key}"
      end

      def file_exists?(date:, asset_class: :stocks, data_type: :day_aggs)
        s3_key = PathBuilder.s3_key(date: date, asset_class: asset_class, data_type: data_type)

        s3_client.head_object(bucket: S3_BUCKET, key: s3_key)
        true
      rescue Aws::S3::Errors::NotFound, Aws::S3::Errors::NoSuchKey
        false
      rescue Aws::S3::Errors::Forbidden, Aws::S3::Errors::AccessDenied
        false
      end

      def file_metadata(date:, asset_class: :stocks, data_type: :day_aggs)
        s3_key = PathBuilder.s3_key(date: date, asset_class: asset_class, data_type: data_type)

        response = s3_client.head_object(bucket: S3_BUCKET, key: s3_key)

        {
          size: response.content_length,
          last_modified: response.last_modified,
          etag: response.etag,
        }
      rescue Aws::S3::Errors::NotFound, Aws::S3::Errors::NoSuchKey
        raise FileNotFoundError, "File not found: #{s3_key}"
      rescue Aws::S3::Errors::Forbidden, Aws::S3::Errors::AccessDenied
        raise PermissionError, "Access denied to file: #{s3_key}"
      end

      def stream_file(date:, asset_class: :stocks, data_type: :day_aggs)
        s3_key = PathBuilder.s3_key(date: date, asset_class: asset_class, data_type: data_type)

        response = s3_client.get_object(bucket: S3_BUCKET, key: s3_key)
        gz_data = response.body.read

        gz = Zlib::GzipReader.new(StringIO.new(gz_data))
        csv_content = gz.read
        gz.close

        if block_given?
          csv_content.each_line do |line|
            yield line
          end
        else
          csv_content
        end
      rescue Aws::S3::Errors::NoSuchKey
        raise FileNotFoundError, "File not found: #{s3_key}"
      rescue Aws::S3::Errors::Forbidden, Aws::S3::Errors::AccessDenied
        raise PermissionError, "Access denied to file: #{s3_key}"
      end

      def list_dates(year:, month: nil, asset_class: :stocks, data_type: :day_aggs)
        prefix = if month
          "#{PathBuilder.send(:asset_class_prefix, asset_class)}/#{PathBuilder.send(:data_type_directory, data_type)}/#{year}/#{month.to_s.rjust(2, "0")}/"
        else
          "#{PathBuilder.send(:asset_class_prefix, asset_class)}/#{PathBuilder.send(:data_type_directory, data_type)}/#{year}/"
        end

        dates = []
        continuation_token = nil

        loop do
          response = s3_client.list_objects_v2(
            bucket: S3_BUCKET,
            prefix: prefix,
            continuation_token: continuation_token,
          )

          response.contents.each do |object|
            if object.key =~ /(\d{4})-(\d{2})-(\d{2})\.csv\.gz$/
              dates << Date.new($1.to_i, $2.to_i, $3.to_i)
            end
          end

          break unless response.is_truncated

          continuation_token = response.next_continuation_token
        end

        dates.sort
      rescue Aws::S3::Errors::Forbidden, Aws::S3::Errors::AccessDenied
        []
      end

      private

      def s3_client
        @s3_client ||= Aws::S3::Client.new(
          access_key_id: @access_key_id,
          secret_access_key: @secret_access_key,
          endpoint: S3_ENDPOINT,
          region: S3_REGION,
          force_path_style: true,
        )
      end
    end
  end
end
