import requests      # api call
import boto3         #aws
import pandas as pd
import numpy as np
import os               #file system
import json
from io import StringIO
from datetime import datetime
from pandas.io.json import json_normalize 


date = datetime.today().strftime('%Y-%m-%d')
# context.updateVariable("date", date)
bucket = 'com.autodesk.marketing.mars.prd.ue1.matillion-data'
auth_token='MTEyNTkwNXwxNjMyMTU1MTQ3fDBjYmEyZGE0LWMzY2UtNGViZC05NDhkLWRmMzFhNGViN2YyMw=='
url = 'https://api.sproutsocial.com/v1/1125905/analytics/posts'

#########Post Performance Facebook#####################
hed = {'Authorization': 'Bearer ' + auth_token} # authorization header
data = {
  "fields": [
    "created_time",
    "perma_link",
    "text",
    "internal.tags.id",
    "internal.sent_by.id",
    "internal.sent_by.email",
    "internal.sent_by.first_name",
    "internal.sent_by.last_name"
  ],
  "filters": [
    "customer_profile_id.eq(3998126,3998127,3998128,3998129,3998130,3998131,3998132,3998133,3998134,3998135,3998136,3998137,3998138,3998139,3998140,3998141,3998142,3998143,3998144,3998145,3998146,3998147,3998148,3998152,3998153,3998154,3998155,3998156,3998157,3998159,3998160,3998161,3998162,3998163,3998164,3998165,3998166,3998167,3998168,3998169,3998170,3998171,3998172,3998174,3998176,3998177,3998178,3998180,3998181,3998182,3998183,3998185,3998186,3998187,3998188,3998189,3998190,3998191,3998192,3998193,3998194,3998195,3998196,3998198,3998199,3998200,3998201,4006082,4006083,4006084,4138714,4168881,4226304,4486255,4486256,4486257,4486258,4486259,4486260,4486261,4486262,4486263,4486264,4486265,4486266,4486267,4486268,4486269,4561629,4847084,4848789,4848790,4848791,4848792,4848793,4850132,4859251)",
    "created_time.in(2021-10-11T00:00:00..2021-10-17T23:59:59)"
  ],
  "metrics": [
"lifetime.impressions",
"lifetime.impressions_organic",
"lifetime.impressions_viral",
"lifetime.impressions_nonviral",
"lifetime.impressions_paid",
"lifetime.impressions_follower",
"lifetime.impressions_follower_organic",
"lifetime.impressions_follower_paid",
"lifetime.impressions_nonfollower",
"lifetime.impressions_nonfollower_organic",
"lifetime.impressions_nonfollower_paid",
"lifetime.impressions_unique",
"lifetime.impressions_organic_unique",
"lifetime.impressions_viral_unique",
"lifetime.impressions_nonviral_unique",
"lifetime.impressions_paid_unique",
"lifetime.impressions_follower_unique",
"lifetime.impressions_follower_paid_unique",
"lifetime.reactions",
"lifetime.likes",
"lifetime.reactions_love",
"lifetime.reactions_haha",
"lifetime.reactions_wow",
"lifetime.reactions_sad",
"lifetime.reactions_angry",
"lifetime.comments_count",
"lifetime.shares_count",
"lifetime.question_answers",
"lifetime.post_content_clicks",
"lifetime.post_link_clicks",
"lifetime.post_photo_view_clicks",
"lifetime.post_video_play_clicks",
"lifetime.post_content_clicks_other",
"lifetime.negative_feedback",
"lifetime.engagements_unique",
"lifetime.engagements_follower_unique",
"lifetime.post_activity_unique",
"lifetime.reactions_unique",
"lifetime.comments_count_unique",
"lifetime.shares_count_unique",
"lifetime.question_answers_unique",
"lifetime.post_content_clicks_unique",
"lifetime.post_link_clicks_unique",
"lifetime.post_photo_view_clicks_unique",
"lifetime.post_video_play_clicks_unique",
"lifetime.post_other_clicks_unique",
"lifetime.negative_feedback_unique",
"video_length",
"lifetime.video_views",
"lifetime.video_views_organic",
"lifetime.video_views_paid",
"lifetime.video_views_autoplay",
"lifetime.video_views_click_to_play",
"lifetime.video_views_sound_on",
"lifetime.video_views_sound_off",
"lifetime.video_views_10s",
"lifetime.video_views_10s_organic",
"lifetime.video_views_10s_paid",
"lifetime.video_views_10s_autoplay",
"lifetime.video_views_10s_click_to_play",
"lifetime.video_views_10s_sound_on",
"lifetime.video_views_10s_sound_off",
"lifetime.video_views_partial",
"lifetime.video_views_partial_organic",
"lifetime.video_views_partial_paid",
"lifetime.video_views_partial_autoplay",
"lifetime.video_views_partial_click_to_play",
"lifetime.video_views_30s_complete",
"lifetime.video_views_30s_complete_organic",
"lifetime.video_views_30s_complete_paid",
"lifetime.video_views_30s_complete_autoplay",
"lifetime.video_views_30s_complete_click_to_play",
"lifetime.video_views_p95",
"lifetime.video_views_p95_organic",
"lifetime.video_views_p95_paid",
"lifetime.video_views_unique",
"lifetime.video_views_organic_unique",
"lifetime.video_views_paid_unique",
"lifetime.video_views_10s_unique",
"lifetime.video_views_30s_complete_unique",
"lifetime.video_views_p95_organic_unique",
"lifetime.video_views_p95_paid_unique",
"lifetime.video_view_time_per_view",
"lifetime.video_view_time",
"lifetime.video_view_time_organic",
"lifetime.video_view_time_paid",
"lifetime.video_ad_break_impressions",
"lifetime.video_ad_break_earnings",
"lifetime.video_ad_break_cost_per_impression"
  ],
  "timezone": "America/Chicago",
  "page": 1
}

response = requests.post(url, json=data, headers=hed)
print(response)  # status code200,  client error 400  
print(response.json()) # for recieving dict object from api \\ .text
user_data=response.json() # for saving response
print(user_data.keys())

# a key in user data...
if type(user_data['data'][-1]) == list:                          # if last element is list
    userdata_1 = json_normalize(user_data['data'][:-1])
else:
    userdata_1 = json_normalize(user_data['data'])
user_df = pd.DataFrame(userdata_1)
# replace empty list coming from the json to NaN
user_df = user_df.mask(user_df.applymap(str).eq('[]'))  #// check on google //////// applymap is used to apply function on each column
                                                        # pd.mask  Replace values where the condition is True.
# print(user_df.count())

# Drop the Duplicates
user_df = user_df.loc[user_df.astype(str).drop_duplicates().index] #astype convert Data type of series

#print(user_df)

# Store CSV to S3 bucket  # pushing the data to s3 bucket
csv_buffer = StringIO()            # working with text in memory using the file API (read, write. etc.).
user_df.to_csv(csv_buffer, index=False)     # to Write the Pandas DataFrame to AWS S3
output_file_path = 'sprout/post/facebook_'+date+'.csv' # file path in bucket
s3client = boto3.client("s3")  # data push to s3 bucket # to use any services from python we use boto3
response = s3client.put_object(
    Bucket=bucket,
    Key=output_file_path,
  	Body=csv_buffer.getvalue()
)                                   # getvalue() method returns the entire content of the file.

#########Post Performance Twitter#####################

hed = {'Authorization': 'Bearer ' + auth_token}
data = {
 "fields": [
    "created_time",
    "perma_link",
    "text",
    "internal.tags.id",
    "internal.sent_by.id",
    "internal.sent_by.email",
    "internal.sent_by.first_name",
    "internal.sent_by.last_name"
  ],
  "filters": [
    "customer_profile_id.eq(3547333,3936991,3998292,3998297,3998302,3998310,3998353,3998356,3998358,3998363,3998369,3998410,3998576,3998596,3998598,3998602,4000316,4000348,4000396,4000476,4000561,4000591,4000658,4000662,4000664,4000669,4000672,4000725,4000755,4000859,4000969,4001088,4001192,4002029,4002035,4002062,4002215,4002224,4002838,4002840,4005261,4005280,4005309,4005314,4006005,4006023,4006024,4006034,4006763,4007946,4024761,4038050,4099543,4561875,4628651,4641053,4827100)",
    "created_time.in(2021-10-11T00:00:00..2021-10-17T23:59:59)"
  ],
  "metrics": [
"lifetime.impressions",
"lifetime.post_media_views",
"lifetime.video_views",
"lifetime.reactions",
"lifetime.likes",
"lifetime.comments_count",
"lifetime.shares_count",
"lifetime.post_content_clicks",
"lifetime.post_link_clicks",
"lifetime.post_content_clicks_other",
"lifetime.post_media_clicks",
"lifetime.post_hashtag_clicks",
"lifetime.post_detail_expand_clicks",
"lifetime.post_profile_clicks",
"lifetime.engagements_other",
"lifetime.post_followers_gained",
"lifetime.post_app_engagements",
"lifetime.post_app_installs",
"lifetime.post_app_opens"
  ],
  "timezone": "America/Chicago",
  "page": 1
}

response = requests.post(url, json=data, headers=hed)
print(response)
print(response.json())
user_data=response.json()
if type(user_data['data'][-1]) == list:
    userdata_1 = json_normalize(user_data['data'][:-1])
else:
    userdata_1 = json_normalize(user_data['data'])
user_df = pd.DataFrame(userdata_1)
# replace empty list coming from the json to NaN
user_df = user_df.mask(user_df.applymap(str).eq('[]'))
# print(user_df.count())

# Drop the Duplicates
user_df = user_df.loc[user_df.astype(str).drop_duplicates().index]
#print(user_df)

# Store CSV to S3 bucket
csv_buffer = StringIO()
user_df.to_csv(csv_buffer, index=False)
output_file_path = 'sprout/post/twitter_'+date+'.csv'
s3client = boto3.client("s3")
response = s3client.put_object(
    Bucket=bucket,
    Key=output_file_path,
  	Body=csv_buffer.getvalue()
)

#########Post Performance LinkedIn#####################

hed = {'Authorization': 'Bearer ' + auth_token}
data = {
 
  "fields": [
    "created_time",
    "perma_link",
    "text",
    "internal.tags.id",
    "internal.sent_by.id",
    "internal.sent_by.email",
    "internal.sent_by.first_name",
    "internal.sent_by.last_name"
  ],
  "filters": [
    "customer_profile_id.eq(3998374,3998375,3998376,3998377,3998378,3998379,3998380,3998381,3998382,3998383,3998384,4002532,4006099,4006100,4006101,4561632,4816663,4850135)",
    "created_time.in(2021-10-11T00:00:00..2021-10-17T23:59:59)"
  ],
  "metrics": [
"lifetime.impressions",
"lifetime.reactions",
"lifetime.comments_count",
"lifetime.shares_count",
"lifetime.post_content_clicks",
"lifetime.video_views"
  ],
  "timezone": "America/Chicago",
  "page": 1
}


response = requests.post(url, json=data, headers=hed)
print(response)
print(response.json())
user_data=response.json()
if type(user_data['data'][-1]) == list:
    userdata_1 = json_normalize(user_data['data'][:-1])
else:
    userdata_1 = json_normalize(user_data['data'])
user_df = pd.DataFrame(userdata_1)
# replace empty list coming from the json to NaN
user_df = user_df.mask(user_df.applymap(str).eq('[]'))
# print(user_df.count())

# Drop the Duplicates
user_df = user_df.loc[user_df.astype(str).drop_duplicates().index]
#print(user_df)

# Store CSV to S3 bucket
csv_buffer = StringIO()
user_df.to_csv(csv_buffer, index=False)
output_file_path = 'sprout/post/linkedin_'+date+'.csv'
s3client = boto3.client("s3")
response = s3client.put_object(
    Bucket=bucket,
    Key=output_file_path,
  	Body=csv_buffer.getvalue()
)

#########Post Performance Instagram#####################

hed = {'Authorization': 'Bearer ' + auth_token}
data = {
 
 "fields": [
    "created_time",
    "perma_link",
    "text",
    "internal.tags.id",
    "internal.sent_by.id",
    "internal.sent_by.email",
    "internal.sent_by.first_name",
    "internal.sent_by.last_name"
  ],
  "filters": [
    "customer_profile_id.eq(3998252,3998253,3998254,3998255,3998256,3998257,3998258,3998259,3998260,3998261,3998262,3998263,3998264,3998265,3998266,3998267,3998268,3998269,3998270,3998271,3998272,3998273,4005978,4006106,4006107,4006108,4007674,4007742,4475150,4848801,4848802,4848804,4850133)",
    "created_time.in(2021-10-11T00:00:00..2021-10-17T23:59:59)"
  ],
  "metrics": [
"lifetime.impressions",
"lifetime.impressions_unique",
"lifetime.video_views",
"lifetime.reactions",
"lifetime.likes",
"lifetime.comments_count",
"lifetime.saves",
"lifetime.comments_count",
"lifetime.story_taps_back",
"lifetime.story_taps_forward",
"lifetime.story_exits"
  ],
  "timezone": "America/Chicago",
  "page": 1
}


response = requests.post(url, json=data, headers=hed)
print(response)
print(response.json())
user_data=response.json()
if type(user_data['data'][-1]) == list:
    userdata_1 = json_normalize(user_data['data'][:-1])
else:
    userdata_1 = json_normalize(user_data['data'])
user_df = pd.DataFrame(userdata_1)
# replace empty list coming from the json to NaN
user_df = user_df.mask(user_df.applymap(str).eq('[]'))
# print(user_df.count())

# Drop the Duplicates
user_df = user_df.loc[user_df.astype(str).drop_duplicates().index]
#print(user_df)

# Store CSV to S3 bucket
csv_buffer = StringIO()
user_df.to_csv(csv_buffer, index=False)
output_file_path = 'sprout/post/instagram_'+date+'.csv'
s3client = boto3.client("s3")
response = s3client.put_object(
    Bucket=bucket,
    Key=output_file_path,
  	Body=csv_buffer.getvalue()
)

# how to call api from python scripts
# free available api for testing
# how to push image to s3
# python request library #$$$$$$$$$$
