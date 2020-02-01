import json
import datetime
import boto3
import os
import pathlib

try:
    comprehend = boto3.client('comprehend', region_name='us-east-2')
except:
    print("Setup Comprehend Err: ")
else:
    print("Finish Setup Comprehend")

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
            text = post['shortcode_media']['edge_media_to_caption']['edges'][0]['node']['text']
            text_replaced = text.replace('\n', '.').replace('#', ', ')
            if len(text) > 0:
                self.ext_data.append({
                    "id": post['shortcode_media']['id'],
                    "shortcode": post['shortcode_media']['shortcode'],
                    "owner_id": post['shortcode_media']['owner']['id'],
                    "timestamp": int(post['shortcode_media']['taken_at_timestamp']),
                    "text": text_replaced,
                    "like": int(post['shortcode_media']['edge_media_preview_like']['count']),
                    "comment_count": int(post['shortcode_media']['edge_media_preview_comment']['count']),
                    "language": "",
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
            print("Detect language failed: " + str(e))
        else:
            print("Detect language success")
    
    def keyphrases(self):
        try:
            for i in range(len(self.data)):
                response = comprehend.detect_key_phrases(Text=self.data[i]['text'], LanguageCode=self.data[i]['language'])
                if len(response['KeyPhrases']) > 0:
                    self.data[i]['keyphrases'] = response['KeyPhrases']
        except Exception as e:
            print("Detect keyphrases failed: " + str(e))
        else:
            print("Detect keyphrases success")
            
    def entities(self):
        try:
            for i in range(len(self.data)):
                response = comprehend.detect_entities(Text=self.data[i]['text'], LanguageCode=self.data[i]['language'])
                if len(response['Entities']) > 0:
                    self.data[i]['entities'] = response['Entities']
        except Exception as e:
            print("Detect entities failed: " + str(e))
        else:
            print("Detect entities success")

    def sentiment(self):
        try:
            for i in range(len(self.data)):
                response = comprehend.detect_sentiment(Text=self.data[i]['text'], LanguageCode=self.data[i]['language'])
                if len(response['Sentiment']) > 0:
                    self.data[i]['sentiment'] = response['Sentiment']
        except Exception as e:
            print("Detect sentiment failed: " + str(e))
        else:
            print("Detect sentiment success")


file = pathlib.Path('insta-analyze/data/test_data_full.json')
data = ExtractData(file.resolve())
data.extract()
comprehend_data = Comprehend(data.ext_data)
comprehend_data.language()
#comprehend_data.entities()
comprehend_data.keyphrases()
comprehend_data.sentiment()
