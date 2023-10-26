from googleapiclient.discovery import build
from bs4 import BeautifulSoup
# Arguments that need to passed to the build function
import os
from dotenv import load_dotenv
import pandas as pd
dotenv_path = '.env'
load_dotenv(dotenv_path)
import pickle
DEVELOPER_KEY = os.environ.get("API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


# creating Youtube Resource Object
youtube_object = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
										developerKey = DEVELOPER_KEY)

data_all=[]

def youtube_search_keyword(query, max_results):
	
# 	# calling the search.list method to
# 	# retrieve youtube search results
	search_keyword = youtube_object.search().list(q = query, part = "id, snippet", maxResults = max_results).execute()
	
	# extracting the results from search response
	results = search_keyword.get("items", [])
	


	# empty list to store video,
	# channel, playlist metadata
	videos = []
	playlists = []
	channels = []
	
	# extracting required info from each result object
	for result in results:
		# video result object

		if result['id']['kind'] == "youtube#video":
			videos.append("% s (% s) (% s) (% s)" % (result["snippet"]["title"],result["id"]["videoId"], result['snippet']['description'],result['snippet']['thumbnails']['default']['url']))
			final=video_comments(result["id"]["videoId"])
			data_all.append(final)
			print("==========================")
			
            
            
# 		# playlist result object
# 		elif result['id']['kind'] == "youtube# playlist":
# 			playlists.append("% s (% s) (% s) (% s)" % (result["snippet"]["title"],
# 								result["id"]["playlistId"],
# 								result['snippet']['description'],
# 								result['snippet']['thumbnails']['default']['url']))

# 		# channel result object
# 		elif result['id']['kind'] == "youtube# channel":
# 			channels.append("% s (% s) (% s) (% s)" % (result["snippet"]["title"],
# 								result["id"]["channelId"],
# 								result['snippet']['description'],
# 								result['snippet']['thumbnails']['default']['url']))
		
	print("Videos:\n", "\n\n\n".join(videos), "\n")
# 	print("Channels:\n", "\n".join(channels), "\n")
# 	print("Playlists:\n", "\n".join(playlists), "\n")


def video_comments(video_id):
	# empty list for storing reply
	final=[]
	replies = []
	try:
		video_response=youtube_object.commentThreads().list(
		part='snippet,replies',
		videoId=video_id
		).execute()
	except:
		return []
	

	print(video_response)

	# iterate video response
	while video_response:
		# print(video_response)
		# extracting required info
		# from each result object
		print(len(video_response['items']))
		for item in video_response['items']:
			# Extracting comments
			comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
			updated_at= item['snippet']['topLevelComment']['snippet']['updatedAt']
			
			# counting number of reply of comment
			replycount = item['snippet']['totalReplyCount']
			print(video_id)

			# if reply is there
			if replycount>0:
								
				# iterate through all reply
				for reply in item['replies']['comments']:
					
					# Extract reply
					reply = reply['snippet']['textDisplay']
					print(video_id)
					# Store reply is list
					replies.append(reply)
					final.append(reply)
					# print(reply)

			soup = BeautifulSoup(comment,features="html.parser")
            
			# print(soup.get_text('\n'), replies, end = '\n\n')

			# empty reply list
			final.append(comment)
			replies = []

		# Again repeat
		if 'nextPageToken' in video_response:
			video_response = youtube_object.commentThreads().list(
					part = 'snippet,replies',
					videoId = video_id,
                    pageToken = video_response['nextPageToken']
				).execute()
		else:
			break
	
	return final

if __name__ == "__main__":
	l=["BE Computer Science","Gate Aspirants","GCT Coimbatore"]
	for i in l:
		print(l)
		youtube_search_keyword(i, max_results = 50)

	with open('data_final.pickle', 'wb') as f:
		pickle.dump(data_all, f, pickle.HIGHEST_PROTOCOL)

	with open('data_final.pickle', 'rb') as handle:
		tokenizer = pickle.load(handle)
	
	print(tokenizer)
	# df = pd.DataFrame (data_all, columns = ['text'])
	# df.to_csv('data_final.csv')

	print(len(tokenizer))
	count=0
	final=[]
	for i in tokenizer:
		count=count+len(i)
		final.extend(i)
	print(count)

	df = pd.DataFrame (final, columns = ['text'])
	df.to_csv('data.csv')
	
