#! /usr/bin/env ruby

require 'dstk'
require 'fileutils'
require 'pp'

class Bootcamp
  SFPD_DATA = 'http://apps.sfgov.org/datafiles/view.php?file=Police/sfpd_incident_all_csv.zip'
  DATA_DIR = File.join(File.dirname(__FILE__), '..', 'data', 'sfpd')
  DATA_FILE = 'data.zip'
  DATA_PATH = File.join(DATA_DIR, DATA_FILE)

  attr_reader :dstk

  def initialize
    create_data_dir
    @dstk = DSTK::DSTK.new
  end

  def create_data_dir
    unless File.exists?(DATA_DIR)
      puts 'Creating data directory ...'
      FileUtils.mkpath(DATA_DIR)
    end
  end

  def extract_data
    cmd = "unzip -d #{DATA_DIR} -o #{DATA_PATH}"
    system cmd
  end

  def download_data
    puts 'Downloading data ...'

    cmd = "wget --output-document=#{DATA_PATH} #{SFPD_DATA}"
    system cmd
  end

  def process_files
    Dir[DATA_DIR + '/**/*.csv'].each do |file|
      puts file
    end
  end

  def run(argv = ARGV)
    return run_all(argv) if argv.length == 0

    return run_lookup(argv) if argv[0] == 'lookup'
  end

  def run_all(argv = ARGV)
    # First download data
    download_data

    # Extract data
    extract_data

    # Process files
    process_files
  end

  def run_lookup(argv = ARGV)
    lng = argv[1].to_f
    lat = argv[2].to_f

    res = dstk.coordinates2politics([lat, lng])
    nbh = res[0]['politics'].select { |r| r['friendly_type'] == 'neighborhood'}
    puts "#{lng},#{lat},#{nbh.first['name']}"
  end
end

if __FILE__ == $PROGRAM_NAME
  bootcamp = Bootcamp.new
  bootcamp.run
end
