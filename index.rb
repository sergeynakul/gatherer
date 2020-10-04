# frozen_string_literal: true

require 'faraday'
require 'hashie'
require 'json'
require 'pg'

def get_items_by(link)
  response = Faraday.get(link)
  response_body = JSON.parse(response.body)
  catalog = response_body['catalog']
  catalog.extend Hashie::Extensions::DeepFind
  catalog.deep_find('items')
end

items = get_items_by('https://api.ozon.ru/composer-api.bx/page/json/v1?url=/category/televizory-15528')

(2..).each do |i|
  link = "https://api.ozon.ru/composer-api.bx/page/json/v1?url=/category/televizory-15528?page=#{i}"
  new_items = get_items_by(link)

  break unless new_items

  items += new_items
end

fields = items.map do |item|
  { id: item['cellTrackingInfo']['id'],
    title: item['cellTrackingInfo']['title'],
    price: item['cellTrackingInfo']['price'],
    discount: item['cellTrackingInfo']['discount'],
    final_price: item['cellTrackingInfo']['finalPrice'],
    free_rest: item['cellTrackingInfo']['freeRest'] }
end

uniq_fields = fields.group_by { |item| item[:id] }.values.map do |a|
  { id: a.first[:id],
    title: a.first[:title],
    price: a.first[:price],
    discount: a.first[:discount],
    final_price: a.first[:final_price],
    free_rest: a.inject(0) { |sum, h| sum + h[:free_rest] } }
end

begin
  con = PG.connect dbname: 'postgres', user: 'postgres'
  con.exec 'CREATE DATABASE gathererdb'
  con = PG.connect dbname: 'gathererdb', user: 'postgres'
  con.exec 'CREATE TABLE items(
    id INTEGER PRIMARY KEY,
    title VARCHAR,
    price INT,
    discount INT,
    final_price INT,
    free_rest INT
  )'
  con.exec 'CREATE INDEX title_idx ON items (title)'
  con.exec 'CREATE INDEX free_rest_idx ON items (free_rest)'
  uniq_fields.each do |item|
    con.exec "INSERT INTO items VALUES(
      #{item[:id]},
      '#{item[:title]}',
      #{item[:price]},
      #{item[:discount]},
      #{item[:final_price]},
      #{item[:free_rest]}
    )"
  end
rescue PG::Error => e
  puts e.message
ensure
  con&.close
end
