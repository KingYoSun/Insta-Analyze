import json
import datetime
import boto3
import os
import pathlib
import re

try:
    comprehend = boto3.client('comprehend', region_name='us-east-2')
except:
    print("Setup Comprehend Err: ")
else:
    print("Finish Setup Comprehend")

#テキストからハッシュタグを抜く
def exclude_hashtag(text):
    return re.sub(r'([#＃][Ａ-Ｚａ-ｚA-Za-z一-鿆0-9０-９ぁ-ヶｦ-ﾟー]+)', "", text)

class ExtractData:
    def __init__(self, data_address):
        with open(data_address) as f:
            self.data = json.load(f)
        self.ext_data = [] #抽出後のデータ
        #
        '''
        必要な情報:
        data['shortcode_media']['id']: 投稿ID
        data['shortcode_media']['shortcode']: 投稿ショートコード
        data['shortcode_media']['owner']['id']: 投稿者ID
        data['shortcode_media']['taken_at_timestamp']: 投稿時間UNIX
        data['shortcode_media']['edge_media_to_caption']['edges'][0]['node']['text']: AWS ConpehendでEntity抽出 + 感情分析
        data['shortcode_media']['edge_media_preview_comment']['count']: コメント数
        data['shortcode_media']['edge_media_preview_comment']['edges'][n]['node']['id']: コメントID
        data['shortcode_media']['edge_media_preview_comment']['edges'][n]['node']['owner']['id']: コメント主ID
        data['shortcode_media']['edge_media_preview_comment']['edges'][n]['node']['text']: AWS Comprehendで感情分析
        data['shortcode_media']['edge_media_preview_like']['count']: いいね数
        '''
        
    def extract(self):
        for post in self.data:
            #テキストからハッシュタグ抽出
            if post['shortcode_media']['edge_media_to_caption']['edges'] == []:
                text = ""
                hashtags = []
            else:
                text = post['shortcode_media']['edge_media_to_caption']['edges'][0]['node']['text']
                text_replaced = text.replace('\n', '')
                hashtags = re.findall(r'([#＃][Ａ-Ｚａ-ｚA-Za-z一-鿆0-9０-９ぁ-ヶｦ-ﾟー]+)', text_replaced)
            #除外設定
            negative_keywords = ["#プレゼント", "#プレゼントキャンペーン", "#プレキャン", "#プレゼント企画", "#懸賞", "キャンペーン", "#キャンペーン実施中", "#インスタキャンペーン"]
            if len(text) > 0 and len(text.encode()) < 4950 and (len(set(hashtags) & set(negative_keywords)) == 0):
                self.ext_data.append({
                    "id": post['shortcode_media']['id'],
                    "shortcode": post['shortcode_media']['shortcode'],
                    "owner_id": post['shortcode_media']['owner']['id'],
                    "timestamp": int(post['shortcode_media']['taken_at_timestamp']),
                    "text": text_replaced,
                    "like": int(post['shortcode_media']['edge_media_preview_like']['count']),
                    "comment_count": int(post['shortcode_media']['edge_media_preview_comment']['count']),
                    "hashtags": hashtags,
                    "language": "ja",
                    "sentiment": "",
                    "comments": [],
                    "keyphrases": [],
                    "entities": []
                })
                if self.ext_data[-1]['comment_count'] > 0:
                    for comment in post['shortcode_media']['edge_media_preview_comment']['edges']:
                        self.ext_data[-1]['comments'].append({
                            "id": comment['node']['id'],
                            "owner_id": comment['node']['owner']['id'],
                            "text": comment['node']['text'],
                        })

class Comprehend:
    def __init__(self, data):
        self.data = data
    
    def language(self):
        try:
            for i in range(len(self.data)):
                response = comprehend.detect_dominant_language(Text=self.data[i]['text'])
                self.data[i]['language'] = response['Languages'][0]['LanguageCode']
        except Exception as e:
            raise("Detect language failed: " + str(e))
        else:
            print("Detect language successful")
    
    def keyphrases(self):
        try:
            count = 0
            for i in range(len(self.data)):
                text_without_hashtag = exclude_hashtag(self.data[i]['text'])
                if len(text_without_hashtag) > 0: 
                    response = comprehend.detect_key_phrases(Text=text_without_hashtag, LanguageCode=self.data[i]['language'])
                else:
                    response = {"KeyPhrases": []}
                if len(response['KeyPhrases']) > 0:
                    for j in range(len(response['KeyPhrases'])):
                        self.data[i]['keyphrases'].append(response['KeyPhrases'][j]['Text'])
                count += 1
                print("{} posts recognized(keyphrases)".format(count))
        except Exception as e:
            raise("Detect keyphrases failed: " + str(e))
        else:
            print("Detect keyphrases successful")
            
    def entities(self):
        try:
            count = 0
            for i in range(len(self.data)):
                text_without_hashtag = exclude_hashtag(self.data[i]['text'])
                if len(text_without_hashtag) > 0:
                    response = comprehend.detect_entities(Text=text_without_hashtag, LanguageCode=self.data[i]['language'])
                else:
                    response = {"Entities": []}
                if len(response['Entities']) > 0:
                    self.data[i]['entities'] = response['Entities']
                count += 1
                print("{} posts recognized(entities)".format(count))
        except Exception as e:
            raise("Detect entities failed: " + str(e))
        else:
            print("Detect entities successful")

    def sentiment(self):
        try:
            count = 0
            for i in range(len(self.data)):
                text_without_hashtag = exclude_hashtag(self.data[i]['text'])
                if len(text_without_hashtag) > 0:
                    response = comprehend.detect_sentiment(Text=text_without_hashtag, LanguageCode=self.data[i]['language'])
                else:
                    response = {"Sentiment": ""}
                if len(response['Sentiment']) > 0:
                    self.data[i]['sentiment'] = response['Sentiment']
                count += 1
                print("{} posts recognized(sentiment)".format(count))
        except Exception as e:
            raise("Detect sentiment failed: " + str(e))
        else:
            print("Detect sentiment successful")


file = pathlib.Path('insta-analyze/data/test_data_full.json')
data = ExtractData(file.resolve())
data.extract()
comprehend_data = Comprehend(data.ext_data)
del data
print("Send comprehend...")
#comprehend_data.language()
comprehend_data.entities()
comprehend_data.keyphrases()
comprehend_data.sentiment()

#データをjsonで出力
try:
    filepath = pathlib.Path('insta-analyze/export/data.json')
    with open(filepath, 'w') as f:
        json.dump(comprehend_data.data, f, indent=4, ensure_ascii=False)
except Exception as e:
    print("Export failed: " + str(e))
else:
    print("Export successful")