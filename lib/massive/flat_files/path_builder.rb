require "date"

module Massive
  module FlatFiles
    module PathBuilder
      module_function

      def s3_key(date:, asset_class: :stocks, data_type: :day_aggs)
        date = parse_date(date)

        prefix = asset_class_prefix(asset_class)
        type_dir = data_type_directory(data_type)
        date_path = date_path_component(date, data_type)
        filename = date_filename(date)

        "#{prefix}/#{type_dir}/#{date_path}/#{filename}"
      end

      def local_path(date:, asset_class: :stocks, data_type: :day_aggs, base_dir:)
        raise ArgumentError, "base_dir is required" if base_dir.nil? || base_dir.empty?

        base_dir = File.expand_path(base_dir)

        s3_path = s3_key(date: date, asset_class: asset_class, data_type: data_type)
        File.join(base_dir, s3_path)
      end

      def parse_date(date)
        case date
        when Date
          date
        when String
          Date.parse(date)
        else
          raise ArgumentError, "date must be a Date or String, got #{date.class}"
        end
      end
      private_class_method :parse_date

      def asset_class_prefix(asset_class)
        case asset_class
        when :stocks
          "us_stocks_sip"
        when :options
          "us_options_opra"
        when :crypto
          "global_crypto"
        when :forex
          "global_forex"
        when :indices
          "us_indices"
        else
          raise ArgumentError, "Unknown asset_class: #{asset_class}"
        end
      end
      private_class_method :asset_class_prefix

      def data_type_directory(data_type)
        case data_type
        when :day_aggs
          "day_aggs_v1"
        when :minute_aggs
          "minute_aggs_v1"
        when :trades
          "trades_v1"
        when :quotes
          "quotes_v1"
        else
          raise ArgumentError, "Unknown data_type: #{data_type}"
        end
      end
      private_class_method :data_type_directory

      def date_path_component(date, data_type)
        year = date.year.to_s
        month = date.month.to_s.rjust(2, "0")

        if data_type == :day_aggs
          "#{year}/#{month}"
        else
          day = date.day.to_s.rjust(2, "0")
          "#{year}/#{month}/#{day}"
        end
      end
      private_class_method :date_path_component

      def date_filename(date)
        "#{date.strftime("%Y-%m-%d")}.csv.gz"
      end
      private_class_method :date_filename
    end
  end
end
