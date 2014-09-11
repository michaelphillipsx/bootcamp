#! /usr/bin/env ruby

require 'csv'
require 'dstk'
require 'fileutils'
require 'json'
require 'pp'
require 'rest-client'

require_relative 'erb_helper'

# Bootcamp implementation class
class Bootcamp
  SFPD_DATA = 'http://apps.sfgov.org/datafiles/view.php?file=Police/sfpd_incident_all_csv.zip'
  DATA_DIR = File.join(File.dirname(__FILE__), '..', 'data', 'sfpd')
  DATA_FILE = 'data.zip'
  DATA_PATH = File.join(DATA_DIR, DATA_FILE)
  CACHE_FILE = 'data.db'
  CACHE_PATH = File.join(DATA_DIR, CACHE_FILE)
  IMPORT_PATH = File.join(DATA_DIR, '..', 'locations_filtered_condensed.csv')

  LATEST_INCIDENTS_URL = 'http://apps.sfgov.org/datafiles/view.php?file=Police/incident_changes_from_previous_day.json'

  DSS_INSTANCE_ID = 'w9c4aec5c9944ac5789e8bcc7b352b23'
  DSS_URL = "jdbc:dss://secure.gooddata.com/gdc/dss/instances/#{DSS_INSTANCE_ID}"
  DSS_USERNAME = ENV['GD_GEM_USER']
  DSS_PASSWORD = ENV['GD_GEM_PASSWORD']

  TABLE_LOCATIONS = 'locations_tmp'
  TABLE_SFPD = 'sfpd_tmp'

  SQL_DIR = File.join(File.dirname(__FILE__), '..', 'sql')
  SQL_CREATE_LOCATIONS = File.join(SQL_DIR, 'create_locations.sql.erb')
  SQL_CREATE_SFPD = File.join(SQL_DIR, 'create_sfpd.sql.erb')
  SQL_MERGE_LOCATIONS = File.join(SQL_DIR, 'merge_locations.sql.erb')
  SQL_MERGE_SFPD = File.join(SQL_DIR, 'merge_sfpd.sql.erb')
  SQL_INSERT_LOCATION = File.join(SQL_DIR, 'insert_location.sql.erb')
  SQL_INSERT_SFPD = File.join(SQL_DIR, 'insert_sfpd.sql.erb')

  attr_reader :dstk
  attr_reader :db
  attr_reader :dss

  # Constructor of Bootcamp class
  def initialize
    # Create temporary data directory
    create_data_dir

    # Init connection to ADS
    init_ads

    # Initialize Data science toolkit
    init_dstk

    # Init connection to SQLite
    # init_sqlite3
  end

  # Initialize ADS connection
  # - Load jdbc gem
  # - Load sequel
  # - Connect using credentials from environment variables
  def init_ads
    # Load gem required for ADS
    require 'jdbc/dss'
    require 'sequel'

    # Initialize JDBC ADS driver
    Jdbc::DSS.load_driver
    Java.com.gooddata.dss.jdbc.driver.DssDriver

    puts "Initializing DSS connection to #{DSS_URL} as user #{DSS_USERNAME}"
    @dss = Sequel.connect DSS_URL, :username => DSS_USERNAME, :password => DSS_PASSWORD

    run_dss_script(SQL_CREATE_LOCATIONS, table: TABLE_LOCATIONS)
    run_dss_script(SQL_CREATE_SFPD, table: TABLE_SFPD)
  end

  # Initialize connection to Data Science ToolKit
  def init_dstk
    @dstk = DSTK::DSTK.new
  end

  # Initialize connection to SQLite database
  # - Used for some local caching
  def init_sqlite3
    require 'sqlite3'
    @db = SQLite3::Database.open(CACHE_PATH)
    @db.execute 'CREATE TABLE IF NOT EXISTS coordinates(lng REAL, lat REAL, neighborhood TEXT);'
    @db.execute 'CREATE INDEX IF NOT EXISTS coordinates_idx ON coordinates (lng, lat);'
  end

  # Direct reverse geolookup using DSTK
  # @arg lng Longitude
  # @arg lat Latitude
  def noncached_lookup(lng, lat)
    res = dstk.coordinates2politics([lat, lng])
    nbh = res[0]['politics'] ? res[0]['politics'].select { |r| r['friendly_type'] == 'neighborhood' } : nil
    nbh = nbh.first['name'] if nbh && !nbh.empty?
    nbh = 'N/A' if nbh.nil?
    {
      lng: lng,
      lat: lat,
      neighborhood: nbh
    }
  end

  # Reverse geolookup using SQLite cache first
  # @arg lng Longitude
  # @arg lat Latitude
  def cached_lookup(lng, lat)
    res = @db.execute "SELECT * FROM coordinates where lng = #{lng.to_f} AND lat = #{lat.to_f}"
    if res.empty?
      res = noncached_lookup(lng, lat)
      sql = "INSERT INTO coordinates(lng, lat, neighborhood) VALUES(#{lng.to_f}, #{lat.to_f}, '#{res[:neighborhood]}')"
      @db.execute sql
      return {
        lng: lng,
        lat: lat,
        neighborhood: res[:neighborhood]
      }
    else
      return {
        lng: lng,
        lat: lat,
        neighborhood: res[0][2]
      }
    end
  end

  # Create data dir for SQLite databse
  def create_data_dir
    unless File.exists?(DATA_DIR)
      puts 'Creating data directory ...'
      FileUtils.mkpath(DATA_DIR)
    end
  end

  # Download all zipped SFPD data
  def download_data
    puts 'Downloading data ...'

    cmd = "wget --output-document=#{DATA_PATH} #{SFPD_DATA}"
    system cmd
  end

  # Extract downloaded SFPD data
  def extract_data
    cmd = "unzip -d #{DATA_DIR} -o #{DATA_PATH}"
    system cmd
  end

  # Get latest incidents
  # @return parsed JSON
  def get_latest_incidents
    JSON.parse(RestClient.get LATEST_INCIDENTS_URL)
  end

  # Process one (big) CSV file with SFPD incident
  def process_file(path)
    CSV.open(path, {:headers => true}) do |csv|
      csv.each do |row|
        res = cached_lookup(row['X'].to_f, row['Y'].to_f)
        puts "#{res[:lng]},#{res[:lat]},#{res[:neighborhood]}"
      end
    end
  end

  # Process all (big) CSV files with incidents
  def process_files
    Dir[DATA_DIR + '/**/*.csv'].each do |file|
      process_file(file)
    end
  end

  # Application entry point
  def run(argv = ARGV)
    # Run all if no argument was passed
    return run_all(argv) if argv.length == 0

    # Run import if argument 'import' was passed
    return run_import(argv) if argv[0] == 'import'

    # Fetch latest incidents if argument 'latest' was passed
    return run_latest(argv) if argv[0] == 'latest'

    # Run lookup for lng and lat if argument 'lookup' was passed
    return run_lookup(argv) if argv[0] == 'lookup'

    # Run sfpd import of latest incidents if argument 'sfpd' was passed
    return run_sfpd(argv) if argv[0] == 'sfpd'
  end

  # Run all steps
  def run_all(argv = ARGV)
    # First download data
    # download_data

    # Extract data
    # extract_data

    # Process files
    # process_files

    run_sfpd(argv)
  end

  # Import existing locations into SQLite cache
  def run_import(argv = ARGV)
    buffer = []
    CSV.open(IMPORT_PATH, {:headers => true}) do |csv|
      csv.each do |row|
        res = @db.execute "SELECT * FROM coordinates where lat = #{row[0].to_f} AND lng = #{row[1].to_f}"
        if res.empty?
          sql = "(#{row[0].to_f}, #{row[1].to_f}, '#{row[2]}')"
          buffer << sql
          if buffer.length > 0 && (buffer.length % 500) == 0
            sql = "INSERT INTO coordinates(lat, lng, neighborhood) VALUES " + buffer.join(", \n") + ";"
            @db.execute sql
            print '.'
            buffer = []
          end
        end
      end

      if buffer.length > 0
        sql = "INSERT INTO coordinates(lat, lng, neighborhood) VALUES " + buffer.join(", \n") + ";"
        @db.execute sql
        print '.'
      end
      puts
    end
  end

  # Run ADS script
  def run_dss_script(path, ctx = {})
    run_sql_script(path, dss, ctx)
  end

  # Run command which fetches latest incidents
  def run_latest(argv = ARGV)
    pp get_latest_incidents
  end

  # Lookup for coordinates passed
  def run_lookup(argv = ARGV)
    lng = argv[1].to_f
    lat = argv[2].to_f

    res = dstk.coordinates2politics([lat, lng])
    nbh = res[0]['politics'].select { |r| r['friendly_type'] == 'neighborhood'}
    puts "#{lng},#{lat},#{nbh.first['name']}"
  end

  # Run command which processes new incidents from JSON fetched
  def run_sfpd(argv = ARGV)
    puts 'Getting JSON with latest incidents'
    incidents = get_latest_incidents

    # Process incidents
    incidents['features'].each do |inc|
      pp inc

      lng = inc['properties']['X'].to_f
      lat = inc['properties']['Y'].to_f

      # Do location lookup
      res = noncached_lookup(lng, lat)

      res.merge!(
        :table => TABLE_LOCATIONS,
        :xy => "#{lat};#{lng}"
      )
      pp res

      # Insert into locations, pass lookup result into template
      run_dss_script(SQL_INSERT_LOCATION, res)
      # run_dss_script(SQL_MERGE_LOCATIONS)

      # Insert into sfpd
      res = inc['properties'].merge(:table => TABLE_SFPD)
      pp res
      run_dss_script(SQL_INSERT_SFPD, res)
      # run_dss_script(SQL_MERGE_SFPD)
    end
  end

  # Run SQL script (template) via SQL adapter specified
  def run_sql_script(path, adapter = dss, ctx = {})
    sql = ErbHelper.new.process(path, ctx)
    puts sql
    adapter.run sql
  end

  # Run SQLite script using SQLite adapter
  def run_sqlite_scipt(path, adapter = dss, ctx = {})
    raise 'Not implemented yet!'
  end
end

# Script entry-point when launched from commandline
if __FILE__ == $PROGRAM_NAME
  bootcamp = Bootcamp.new
  bootcamp.run
end
