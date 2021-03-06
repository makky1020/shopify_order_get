#Shopfy

import requests
import pandas as pd
import json
import datetime
import os
import csv
import time
import shopify
import gspread as gs
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import logging

#B列の最終行を取得
def next_available_row(sheet1):
  str_list = list(filter(None, sheet1.col_values(2)))
  return len(str_list)+4

#2つのAPIを記述しないとリフレッシュトークンを3600秒毎に発行し続けなければならない
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

credential = {
  "type": "service_account",
  "project_id": os.environ['SHEET_PROJECT_ID'],
  "private_key_id": os.environ['SHEET_PRIVATE_KEY_ID'],
  "private_key": os.environ['SHEET_PRIVATE_KEY'].replace('\\n', '\n'),
  "client_email":os.environ['SHEET_CLIENT_EMAIL'],
  "client_id": os.environ['SHEET_CLIENT_ID'],
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": os.environ['SHEET_CLIENT_X509_CERT_URL'] 
}

#認証情報設定
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credential, scope)

#OAuth2の資格情報を使用してGoogle APIにログイン
gc = gs.authorize(credentials)

#共有設定したスプレッドシートキーを変数[SPREADSHEET_KEY]に格納
SPREADSHEET_KEY = os.environ['SPREADSHEET_KEY']

#共有設定したスプレッドシートのワークシートを開く
worksheet = gc.open_by_key(SPREADSHEET_KEY).worksheet('Shopify')
worksheet2 = gc.open_by_key(SPREADSHEET_KEY).worksheet('Litto')

#next_rowに最終行を代入
next_row = next_available_row(worksheet)
#注文IDの最終行を取得
id_row = next_row-1 

#S列の注文IDがあるところまで行を上げる(BASE対策)
while True:
  if worksheet.cell(id_row,19).value == None:
    id_row -= 1
  else:
    break

API_KEY = os.environ['API_KEY']
API_PASS = os.environ['API_PASS']

df = pd.DataFrame([],columns=('注文番号','注文日','金額','個数','商品名','商品コード','配送先氏名','なし','ストリート1','ストリート2','会社名','市区町村','郵便番号','都道府県','電話番号','要望','決済方法','注文ID','商品ID','画像'))

#スプレッドシートの最終注文ID以降の注文を抽出
shop_url = "https://%s:%s@b-right-golf.myshopify.com/admin/api/2022-01/orders.json?since_id=" % (API_KEY, API_PASS) + worksheet.cell(id_row,19).value

res = requests.get(shop_url)
token = res.json()

i = 0
x = 0
for order in token['orders']:
  for item in order['line_items']:
    df.loc[i,'注文番号'] = order['name']
    print(df.loc[i,'注文番号'])
    df.loc[i,'注文日'] = order['created_at']
    #複数注文がある場合、1番上の商品代金のみ送料をプラスする(送料無料の場合は0円)
    if x == 0:
      df.loc[i,'金額'] = int(item['price']) + int(order['shipping_lines'][0]['price']) - int(order['current_total_discounts'])
    else:
      df.loc[i,'金額'] = item['price']

    df.loc[i,'個数'] = item['quantity']
    df.loc[i,'商品名'] = item['name']
    df.loc[i,'商品コード'] = item['sku']
    df.loc[i,'商品ID'] = item['product_id']
    df.loc[i,'配送先氏名'] = order['shipping_address']['name']
    df.loc[i,'ストリート1'] = order['shipping_address']['address1']
    df.loc[i,'ストリート2'] = order['shipping_address']['address2']
    df.loc[i,'会社名'] = order['shipping_address']['company']
    df.loc[i,'市区町村'] = order['shipping_address']['city']
    df.loc[i,'郵便番号'] = order['shipping_address']['zip']

    if order['shipping_address']['province'] == 'Aichi':
      df.loc[i,'都道府県'] = '愛知県'
    elif order['shipping_address']['province'] == 'Fukuoka':
      df.loc[i,'都道府県'] = '福岡県'
    elif order['shipping_address']['province'] == 'Ōsaka':
      df.loc[i,'都道府県'] = '大阪府'
    elif order['shipping_address']['province'] == 'Mie':
      df.loc[i,'都道府県'] = '三重県'
    elif order['shipping_address']['province'] == 'Kanagawa':
      df.loc[i,'都道府県'] = '神奈川県'
    elif order['shipping_address']['province'] == 'Tōkyō':
      df.loc[i,'都道府県'] = '東京都'
    elif order['shipping_address']['province'] == 'Wakayama':
      df.loc[i,'都道府県'] = '和歌山県'
    elif order['shipping_address']['province'] == 'Kyōto':
      df.loc[i,'都道府県'] = '京都府'
    elif order['shipping_address']['province'] == 'Hokkaidō':
      df.loc[i,'都道府県'] = '北海道'
    elif order['shipping_address']['province'] == 'Hyōgo':
      df.loc[i,'都道府県'] = '兵庫県'
    elif order['shipping_address']['province'] == 'Gifu':
      df.loc[i,'都道府県'] = '岐阜県'
    elif order['shipping_address']['province'] == 'Miyagi':
      df.loc[i,'都道府県'] = '宮城県'
    elif order['shipping_address']['province'] == 'Nagasaki':
      df.loc[i,'都道府県'] = '長崎県'
    elif order['shipping_address']['province'] == 'Tochigi':
      df.loc[i,'都道府県'] = '栃木県'
    elif order['shipping_address']['province'] == 'Tokushima':
      df.loc[i,'都道府県'] = '徳島県'
    elif order['shipping_address']['province'] == 'Saga':
      df.loc[i,'都道府県'] = '佐賀県'
    elif order['shipping_address']['province'] == 'Saitama':
      df.loc[i,'都道府県'] = '埼玉県'
    elif order['shipping_address']['province'] == 'Akita':
      df.loc[i,'都道府県'] = '秋田県'
    elif order['shipping_address']['province'] == 'Yamaguchi':
      df.loc[i,'都道府県'] = '山口県'
    elif order['shipping_address']['province'] == 'Chiba':
      df.loc[i,'都道府県'] = '千葉県'
    elif order['shipping_address']['province'] == 'Shizuoka':
      df.loc[i,'都道府県'] = '静岡県'
    elif order['shipping_address']['province'] == 'Gunma':
      df.loc[i,'都道府県'] = '群馬県'
    elif order['shipping_address']['province'] == 'Tottori':
      df.loc[i,'都道府県'] = '鳥取県'
    elif order['shipping_address']['province'] == 'Nagano':
      df.loc[i,'都道府県'] = '長野県'
    elif order['shipping_address']['province'] == 'Nara':
      df.loc[i,'都道府県'] = '奈良県'
    elif order['shipping_address']['province'] == 'Niigata':
      df.loc[i,'都道府県'] = '新潟県'
    elif order['shipping_address']['province'] == 'Fukui':
      df.loc[i,'都道府県'] = '福井県'
    elif order['shipping_address']['province'] == 'Kagoshima':
      df.loc[i,'都道府県'] = '鹿児島県'
    elif order['shipping_address']['province'] == 'Shiga':
      df.loc[i,'都道府県'] = '滋賀県'
    elif order['shipping_address']['province'] == 'Okayama':
      df.loc[i,'都道府県'] = '岡山県'
    elif order['shipping_address']['province'] == 'Hiroshima':
      df.loc[i,'都道府県'] = '広島県'
    elif order['shipping_address']['province'] == 'Okinawa':
      df.loc[i,'都道府県'] = '沖縄県'
    elif order['shipping_address']['province'] == 'Ishikawa':
      df.loc[i,'都道府県'] = '石川県'
    elif order['shipping_address']['province'] == 'Yamaguchi':
      df.loc[i,'都道府県'] = '山口県'
    elif order['shipping_address']['province'] == 'Miyazaki':
      df.loc[i,'都道府県'] = '宮崎県'
    elif order['shipping_address']['province'] == 'Shiga':
      df.loc[i,'都道府県'] = '滋賀県'
    elif order['shipping_address']['province'] == 'Toyama':
      df.loc[i,'都道府県'] = '富山県'
    elif order['shipping_address']['province'] == 'Kumamoto':
      df.loc[i,'都道府県'] = '熊本県'
    elif order['shipping_address']['province'] == 'Kagawa':
      df.loc[i,'都道府県'] = '香川県'
    elif order['shipping_address']['province'] == 'Ibaraki':
      df.loc[i,'都道府県'] = '茨城県'
    else:
      df.loc[i,'都道府県'] = order['shipping_address']['province']

    df.loc[i,'電話番号'] = order['shipping_address']['phone']
    df.loc[i,'要望'] = order['note']
    df.loc[i,'決済方法'] = order['payment_gateway_names'][0]
    df.loc[i,'注文ID'] = order['id']


    #商品IDとSKUをもとに画像IDを探す
    shop_url2 = "https://%s:%s@b-right-golf.myshopify.com/admin/api/2022-01/products/" % (API_KEY, API_PASS) +str(item['product_id'])+".json"
    res2 = requests.get(shop_url2)
    token2 = res2.json()

    try:
      for img_ids in token2['product']['variants']:
        if img_ids['sku'] == item['sku']:
          img_id = img_ids['image_id']
    except KeyError:
      continue

    #画像IDを元に商品画像URLを探す
    shop_url3 = "https://%s:%s@b-right-golf.myshopify.com/admin/api/2022-01/products/" % (API_KEY, API_PASS) +str(item['product_id'])+"/images/"+str(img_id)+".json"
    res3 = requests.get(shop_url3)
    token3 = res3.json()

    df.loc[i,'画像'] = token3['image']['src']

    i += 1
    x += 1
  x = 0

print(df)

#スプレッドシートに出力(行名は出力しない)
set_with_dataframe(worksheet, df, row=int(next_row), col=2, include_column_header=False)