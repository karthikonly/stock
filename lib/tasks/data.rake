require 'net/http'
require 'zip'
require 'csv'

namespace :data do
  desc "cleanup the tmp folder of all zip files"
  task cleanup: :environment do
    sh "rm tmp/*.zip"
  end

  desc "Download bhavcopy data from NSE"
  task fetch: :environment do
    start_date = 1.year.ago.to_date
    end_date = Date.today.to_date

    date_list = (start_date..end_date).map { |d| { str: d.strftime("%d%b%Y").upcase, year: d.strftime("%Y"), month: d.strftime("%b").upcase } }

    date_list.each do |date|
      source_url = URI("https://www.nseindia.com/content/historical/EQUITIES/#{date[:year]}/#{date[:month]}/cm#{date[:str]}bhav.csv.zip")
      puts "fetching: #{source_url}"
      zipped_folder = Net::HTTP.get(source_url)
      File.open("tmp/#{date[:str]}.zip", 'wb') do |file|
        file.write(zipped_folder)
      end
    end
  end

  desc "list zip files"
  task list: :environment do
    # files_list = Dir.glob('tmp/*.zip')
    files_list = `ls -l tmp/*.zip | awk '{print $9, $5}'`.split("\n")
    files_list.each do |file_entry|
      file_name = file_entry.split(' ')[0]
      file_size = file_entry.split(' ')[1].to_i
      next if file_size < 1000
      puts "#{file_name}:#{file_size}"
    end
  end

  desc "unzip zip files"
  task unzip: :environment do
    files_list = `ls -l tmp/*.zip | awk '{print $9, $5}'`.split("\n")
    files_list.each do |file_entry|
      full_file_name = file_entry.split(' ')[0]
      file_size = file_entry.split(' ')[1].to_i
      next if file_size < 1000
      file_name = full_file_name.split('.')[0]
      zip_file = Zip::File.open(full_file_name)
      zip_file.each do |file|
        puts "writing #{file_name}.csv"
        file.extract("#{file_name}.csv")
      end
    end
  end

  desc "load csv files"
  task load_csv: :environment do 
    csv_files = `ls tmp/*.csv`.split("\n")
    # puts csv_file
    csv_files.each do |file|
      date = file.split('/')[1].split('.')[0]
      rows = CSV.read(file, headers: true) 
      rows.each do |row|
        process_row date, row
      end
    end
  end

  private

    def process_row date, row
      puts "#{date}-#{row['SYMBOL']}-#{row['OPEN']}-#{row['HIGH']}-#{row['LOW']}-#{row['CLOSE']}"
    end
end
