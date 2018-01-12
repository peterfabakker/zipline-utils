#!/usr/bin/env ruby

require 'rubygems'
require 'nokogiri'
require 'open-uri'
require 'csv'

doc = Nokogiri::HTML(open("http://vixcentral.com/historical/?days=3000"))
csv = CSV.open("/var/www/html/noko.csv", 'w',{:col_sep => ",", :quote_char => '\'', :force_quotes => false})

doc.xpath('//table//tr').each do |row|
  tarray = []
  row.xpath('td').each do |cell|
    tarray << cell.text
  end
  csv << tarray
end

csv.close
