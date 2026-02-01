require_relative "flat_files/version"
require_relative "flat_files/path_builder"
require_relative "flat_files/client"

module Massive
  module FlatFiles
    S3_ENDPOINT = "https://files.massive.com"
    S3_BUCKET = "flatfiles"
    S3_REGION = "us-east-1"

    ENV_LOCAL_DIR_KEY = "MASSIVE_FLAT_FILES_DIR"

    class Error < StandardError; end
    class PermissionError < Error; end
    class HistoricalDataError < PermissionError; end
    class FileNotFoundError < Error; end
    class CredentialError < Error; end
    class ConfigurationError < Error; end

    module_function

    def sync(date:, local_dir: nil, access_key_id: nil, secret_access_key: nil,
             asset_class: :stocks, data_type: :day_aggs)
      client = get_client(access_key_id: access_key_id, secret_access_key: secret_access_key)

      local_path = PathBuilder.local_path(
        date: date,
        asset_class: asset_class,
        data_type: data_type,
        base_dir: resolve_local_dir(local_dir),
      )

      client.download_file(
        date: date,
        local_path: local_path,
        asset_class: asset_class,
        data_type: data_type,
      )
    end

    def read(date:, local_dir: nil, asset_class: :stocks, data_type: :day_aggs)
      local_path = PathBuilder.local_path(
        date: date,
        asset_class: asset_class,
        data_type: data_type,
        base_dir: resolve_local_dir(local_dir),
      )

      raise FileNotFoundError, "File not found locally: #{local_path}" unless File.exist?(local_path)

      read_csv_gz(local_path)
    end

    def fetch(date:, access_key_id: nil, secret_access_key: nil,
              asset_class: :stocks, data_type: :day_aggs, &block)
      client = get_client(access_key_id: access_key_id, secret_access_key: secret_access_key)

      csv_content = client.stream_file(
        date: date,
        asset_class: asset_class,
        data_type: data_type,
      )

      parse_csv_content(csv_content, &block)
    end

    def list_remote(year:, month: nil, access_key_id: nil, secret_access_key: nil,
                    asset_class: :stocks, data_type: :day_aggs)
      client = get_client(access_key_id: access_key_id, secret_access_key: secret_access_key)

      client.list_dates(
        year: year,
        month: month,
        asset_class: asset_class,
        data_type: data_type,
      )
    end

    def file_exists?(date:, access_key_id: nil, secret_access_key: nil,
                     asset_class: :stocks, data_type: :day_aggs)
      client = get_client(access_key_id: access_key_id, secret_access_key: secret_access_key)

      client.file_exists?(
        date: date,
        asset_class: asset_class,
        data_type: data_type,
      )
    end

    def local_path(date:, local_dir: nil, asset_class: :stocks, data_type: :day_aggs)
      PathBuilder.local_path(
        date: date,
        asset_class: asset_class,
        data_type: data_type,
        base_dir: resolve_local_dir(local_dir),
      )
    end

    class << self
      private

      def resolve_local_dir(local_dir)
        dir = local_dir || ENV[ENV_LOCAL_DIR_KEY]

        if dir.nil? || dir.empty?
          raise ConfigurationError,
            "local_dir is required. Pass local_dir: parameter or set #{ENV_LOCAL_DIR_KEY} environment variable."
        end

        dir
      end

      def get_client(access_key_id: nil, secret_access_key: nil)
        creds = resolve_credentials(
          access_key_id: access_key_id,
          secret_access_key: secret_access_key,
        )

        Client.new(
          access_key_id: creds[:access_key_id],
          secret_access_key: creds[:secret_access_key],
        )
      end

      def resolve_credentials(access_key_id: nil, secret_access_key: nil)
        if access_key_id && secret_access_key
          {
            access_key_id: access_key_id,
            secret_access_key: secret_access_key,
          }
        else
          require "massive/account"

          account_info = Massive::Account.info
          creds = account_info[:credential_sets]&.first

          raise CredentialError, "No credentials found in Massive::Account.info" unless creds

          s3_creds = creds[:s3]
          raise CredentialError, "No S3 credentials in account" unless s3_creds

          {
            access_key_id: s3_creds[:access_key_id],
            secret_access_key: s3_creds[:secret_access_key],
          }
        end
      end

      def read_csv_gz(file_path)
        require "zlib"
        require "csv"
        require "stringio"

        File.open(file_path, "rb") do |f|
          gz = Zlib::GzipReader.new(f)
          csv_content = gz.read
          gz.close

          parse_csv_content(csv_content)
        end
      end

      def parse_csv_content(csv_content)
        require "csv"

        rows = []
        CSV.parse(csv_content, headers: true, header_converters: :symbol) do |row|
          hash = row.to_h

          hash[:volume] = hash[:volume].to_i if hash[:volume]
          hash[:open] = hash[:open].to_f if hash[:open]
          hash[:close] = hash[:close].to_f if hash[:close]
          hash[:high] = hash[:high].to_f if hash[:high]
          hash[:low] = hash[:low].to_f if hash[:low]
          hash[:window_start] = hash[:window_start].to_i if hash[:window_start]
          hash[:transactions] = hash[:transactions].to_i if hash[:transactions]

          if block_given?
            yield hash
          else
            rows << hash
          end
        end

        block_given? ? nil : rows
      end
    end
  end
end
