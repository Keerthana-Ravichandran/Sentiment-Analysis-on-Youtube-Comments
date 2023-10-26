from google_trans import translate
from prefect import Flow,task
import nltk
import re
# from nltk.corpus import stopwords
from textblob import TextBlob
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
# from prefect.schedules import CronSchedule
wl = WordNetLemmatizer()
ps = PorterStemmer()

import numpy as np

# from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.utils import to_categorical
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import one_hot
from tensorflow.keras.models import load_model


from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import datetime
import dateutil.parser
import pytz

dotenv_path = '.env'
load_dotenv(dotenv_path)

DEVELOPER_KEY = os.environ.get("API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


# creating Youtube Resource Object
youtube_object = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
										developerKey = DEVELOPER_KEY)

video_ids=["7slK6LRQvJE"]

# video_ids=["Z7btuaEWjOQ"]

def connect_to_db():
	conn = psycopg2.connect(
	   database=os.getenv('DB_MAIN'), user=os.getenv('USER_MAIN'), password=os.getenv('PASSWORD_MAIN'), host=os.getenv('HOST_MAIN'), port= '5432'
	)
	cur = conn.cursor()
	return cur, conn

# @task
def youtube_title_description(video_ids):
	for video_id in video_ids:
		video_request=youtube_object.videos().list(
			part='snippet,statistics',
			id=video_id
		)
		video_response = video_request.execute()
		title=video_response['items'][0]['snippet']['title']
		description=video_response['items'][0]['snippet']['description']
		cur,conn = connect_to_db()
		if(pd.read_sql(f'''SELECT * from public.videos where video_id='{video_id}' and title='{title.replace("'","")}' and description='{description.replace("'","")}' ''', con=conn).empty):
			if(pd.read_sql(f'''SELECT * from public.videos where video_id='{video_id}' ''', con=conn).empty):
				query = f"""INSERT INTO  public.videos (video_id,title,description) VALUES ('{video_id}','{title.replace("'","")}','{description.replace("'","")}')"""
				cur.execute(query)
				print("insert")
				conn.commit()
			else:
				query = f"""update public.videos set title='{title.replace("'","")}',description='{description.replace("'","")}' where video_id='{video_id}'"""
				cur.execute(query)
				conn.commit()
				print("update")
		else:
			print("Not")

		video_comments(video_id)

		cur.close()
		conn.close()



def get_maxlen(corpus):
	maxlen = 0
	for sent in corpus:
		maxlen = max(maxlen, len(sent))
	return maxlen

def word_embedding(texts):
	MAX_NB_WORDS = 40000
	stopwords=['a',
	'about',
	'again',
	'against',
	'ain',
	'all',
	'am',
	'an',
	'and',
	'any',
	'are',
	'aren',
	"aren't",
	'as',
	'at',
	'be',
	'been',
	'being',
	'between',
	'both',
	'but',
	'by',
	'can',
	'couldn',
	"couldn't",
	'd',
	'did',
	'didn',
	"didn't",
	'do',
	'does',
	'doesn',
	"doesn't",
	'doing',
	'don',
	"don't",
	'for',
	'from',
	'further',
	'had',
	'hadn',
	"hadn't",
	'has',
	'hasn',
	"hasn't",
	'have',
	'haven',
	"haven't",
	'having',
	'he',
	'her',
	'here',
	'hers',
	'herself',
	'him',
	'himself',
	'his',
	'how',
	'i',
	'if',
	'in',
	'into',
	'is',
	'isn',
	"isn't",
	'it',
	"it's",
	'its',
	'itself',
	'just',
	'll',
	'm',
	'ma',
	'me',
	'mightn',
	"mightn't",
	'more',
	'most',
	'mustn',
	"mustn't",
	'my',
	'myself',
	'needn',
	"needn't",
	'nor',
	'o',
	'of',
	'on',
	'once',
	'only',
	'or',
	'other',
	'our',
	'ours',
	'ourselves',
	'out',
	'over',
	'own',
	're',
	's',
	'shan',
	"shan't",
	'she',
	"she's",
	'should',
	"should've",
	'shouldn',
	"shouldn't",
	'so',
	'some',
	'such',
	't',
	'than',
	'that',
	"that'll",
	'the',
	'their',
	'theirs',
	'them',
	'themselves',
	'then',
	'there',
	'these',
	'they',
	'this',
	'those',
	'through',
	'to',
	'too',
	'under',
	'until',
	'up',
	've',
	'very',
	'was',
	'wasn',
	"wasn't",
	'we',
	'were',
	'weren',
	"weren't",
	'what',
	'when',
	'where',
	'which',
	'while',
	'who',
	'whom',
	'why',
	'will',
	'with',
	'won',
	"won't",
	'wouldn',
	"wouldn't",
	'y',
	'you',
	"you'd",
	"you'll",
	"you're",
	"you've",
	'your',
	'yours',
	'yourself',
	'yourselves']
	nltk.download('stopwords')
	nltk.download('punkt')
	nltk.download('wordnet')
	nltk.download('omw-1.4')

	corpus = []
	for i in range(0, len(texts)):
		print(i)
		review = texts[i].split()
		review= [word for word in review if not word.startswith('@') ]
		review = ' '.join(review)
		
		review = re.sub('[^a-zA-Z]', ' ',review)
		review = review.lower()
		
		review= str(TextBlob(review).correct())
		
		review = word_tokenize(review)
		final=[]
		for word in review:
			if word.endswith("'t"):
				word='not'
			if word not in stopwords:
				final.append(wl.lemmatize(word,pos='v'))
		
		final = ' '.join(final)
		print(texts[i])
		print(final)
		corpus.append(final)
	onehot_repr=[one_hot(words,MAX_NB_WORDS)for words in corpus] 
	maxlen=get_maxlen(corpus)
	embedded_docs=pad_sequences(onehot_repr,padding='pre',maxlen=127)
	data=np.array(embedded_docs)
	return data


def model_predict(data):
	model = load_model("sentiment_analysis_model.h5")
	y_pred = model.predict(data)
	y_pred_class = np.argmax(y_pred,axis=1)
	unique = np.array(np.unique(y_pred_class,return_counts=True)).T
	uniques, counts = np.unique(y_pred_class, return_counts=True)
	print("counts: ",counts)
	print("y_pred_class: ",y_pred_class)
	return y_pred_class, counts

		


def video_comments(video_id):
	# empty list for storing reply
	replies = []
	replies_id=[]

	# retrieve youtube video results
	video_response=youtube_object.commentThreads().list(
	part='snippet,replies',
	videoId=video_id
	).execute()

	final=[]
	ids=[]

	comments_all=[]
	replies_all=[]

	# iterate video response
	while video_response:
		for item in video_response['items']:
			# Extracting comments
			comment = item['snippet']['topLevelComment']['snippet']['textDisplay'].replace("'","")
			updated_at= item['snippet']['topLevelComment']['snippet']['updatedAt']
			# counting number of reply of comment
			replycount = item['snippet']['totalReplyCount']
			comment_id=item['id']

			insertion_date = dateutil.parser.parse(updated_at)
			diffretiation = pytz.utc.localize(datetime.datetime.utcnow()) - insertion_date

			cur,conn = connect_to_db()
			if(pd.read_sql(f'''SELECT * from public.comments where comment_id='{comment_id}' ''', con=conn).empty):
				query = f"""INSERT INTO  public.comments (video_id,comment_id,content) VALUES ('{video_id}','{comment_id}','{comment}')"""
				cur.execute(query)
				print("comment insert")
				soup = BeautifulSoup(comment,features="html.parser")
			
			# print(soup.get_text('\n'), ",",replies, end = '\n\n')
				ids.append(comment_id)
				final.append(translate(soup.get_text('\n')))
				conn.commit()
			elif(diffretiation.total_seconds()//60<3000):
				query = f"""update public.comments set content='{comment}' where comment_id='{comment_id}'"""
				cur.execute(query)
				soup = BeautifulSoup(comment,features="html.parser")
			
				# print(soup.get_text('\n'), ",",replies, end = '\n\n')
				ids.append(comment_id)
				final.append(translate(soup.get_text('\n')))
				conn.commit()
				print("comment update")
			else:
				print("comment not")

			comments_all.append(comment_id)


			# if reply is there
			if replycount>0:
								
				# iterate through all reply
				for reply in item['replies']['comments']:
					
					reply_content = reply['snippet']['textDisplay'].replace("'","")
					# Store reply is list
					
					reply_id=reply['id']
					replies_all.append(reply_id)
					
					reply_updated_at= reply['snippet']['updatedAt']
					
					reply_insertion_date = dateutil.parser.parse(reply_updated_at)
					reply_diffretiation = pytz.utc.localize(datetime.datetime.utcnow()) - reply_insertion_date

					if(pd.read_sql(f'''SELECT * from public.replies where reply_id='{reply_id}' ''', con=conn).empty):
						query = f"""INSERT INTO  public.replies (video_id,reply_id,content) VALUES ('{video_id}','{reply_id}','{reply_content}')"""
						cur.execute(query)
						print("reply insert")
						replies_id.append(reply_id)
						replies.append(reply_content)
						conn.commit()
					elif(reply_diffretiation.total_seconds()//60<3000):
						query = f"""update public.replies set content='{reply_content}' where reply_id='{reply_id}'"""
						cur.execute(query)
						conn.commit()
						replies_id.append(reply_id)
						replies.append(reply_content)
						print("reply update")
					else:
						print("reply not")

					

			
			for reply in replies:
				soup = BeautifulSoup(reply,features="html.parser")
				print("=====================================")
				
				final.append(translate(soup.get_text('\n')))

			for reply_id in replies_id:
				ids.append(reply_id)

			# empty reply list
			replies = []
			replies_id=[]

		# Again repeat
		if 'nextPageToken' in video_response:
			video_response = youtube_object.commentThreads().list(
					part = 'snippet,replies',
					videoId = video_id,
					pageToken = video_response['nextPageToken']
				).execute()
		else:
			break

		cur.close()
		conn.close()
	
	cur,conn = connect_to_db()
	replies_all_df=pd.read_sql(f'''SELECT reply_id from public.replies where video_id='{video_id}' ''', con=conn)
	replies_all_db = []
	for i in replies_all_df['reply_id']:
		replies_all_db.append(i)
	print(replies_all_db)


	comments_all_df=pd.read_sql(f'''SELECT comment_id from public.comments where video_id='{video_id}' ''', con=conn)
	comments_all_db = []
	for i in comments_all_df['comment_id']:
		comments_all_db.append(i)
	print(comments_all_db)
	

	comments_all = list(set(comments_all_db).difference(set(comments_all)))
	replies_all = list(set(replies_all_db).difference(set(replies_all)))
	print(comments_all)
	print(replies_all)


	if(comments_all!=[]):
		for i in comments_all:
			query = f"""delete from public.comments where comment_id='{i}'"""
			cur.execute(query)
			conn.commit()
	
	if(replies_all!=[]):
		for i in replies_all:
			query = f"""delete from public.replies where reply_id='{i}'"""
			cur.execute(query)
			conn.commit()
	
	cur.close()
	conn.close()
	

	if(final!=[] or ids!=[]):
		video_embedded=word_embedding(final)
		print(video_embedded,len(video_embedded))
		y_pred_class,counts=model_predict(video_embedded)
		total=np.sum(counts)
		counts_perc=counts/total
		print("counts: ",counts_perc)
		print("rep_ids:",ids)

		cur,conn = connect_to_db()
		for i in range(len(y_pred_class)):
			
			query = f"""update public.comments set value='{y_pred_class[i]}' where comment_id='{ids[i]}'"""
			cur.execute(query)
			conn.commit()
			query = f"""update public.replies set value='{y_pred_class[i]}' where reply_id='{ids[i]}'"""
			cur.execute(query)
			conn.commit()

		replies_value=pd.read_sql(f'''SELECT value from public.replies where video_id='{video_id}' ''', con=conn)
		comments_value=pd.read_sql(f'''SELECT value from public.comments where video_id='{video_id}' ''', con=conn)
		final_data = pd.concat([replies_value, comments_value], axis=0).values.tolist()
		print(final_data)
		uniques, counts = np.unique(final_data, return_counts=True)
		print(counts)
		# query = f"""update public.videos set extremely_happy='{counts[0]}', happy='{counts[1]}' , sad='{counts[3]}' ,neutral='{counts[2]}' ,angry='{counts[4]}' where video_id='{video_id}'"""
		query = f"""update public.videos set question='{counts[0]}', happy='{counts[1]}' , request='{counts[3]}' ,information='{counts[2]}' ,notsatisfied='{counts[4]}' where video_id='{video_id}'"""
		cur.execute(query)
		conn.commit()
		
		cur.close()
		conn.close()
	
# scheduler = CronSchedule(cron='*/5  * * * *')
# scheduler = CronSchedule(cron='*/30 * * * *')

# with Flow("task_automation", schedule = scheduler) as flow:
# 	youtube_title_description(video_ids)

# flow.run()

youtube_title_description(video_ids)
