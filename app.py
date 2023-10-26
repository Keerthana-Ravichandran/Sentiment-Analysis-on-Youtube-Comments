from flask import Flask,render_template,request,Response
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
app = Flask(__name__)
import pandas as pd
import psycopg2

import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

import numpy as np

import os
from dotenv import load_dotenv

dotenv_path = '.env'
load_dotenv(dotenv_path)


final_range={1:[],2:[],3:[],4:[],5:[]}

def connect_to_db():
	conn = psycopg2.connect(
	   database=os.getenv('DB_MAIN'), user=os.getenv('USER_MAIN'), password=os.getenv('PASSWORD_MAIN'), host=os.getenv('HOST_MAIN'), port= '5432'
	)
	cur = conn.cursor()
	return cur, conn

def youtube_search_keyword(category):
	cur,conn = connect_to_db()
	print(cur,conn)
	data = pd.read_sql(f'''SELECT * from public.videos ''', con=conn)
	print(category,data)
	final_return=[]

	for index, row in data.iterrows():
		# counts=[row["happy"],row["information"],row["notsatisfied"],row["irrelevant"],row["question"],row["request"]]
		counts=[row["happy"],row["information"],row["notsatisfied"],row["question"],row["request"]]
		total=np.sum(counts)
		counts=counts/total
		final_range[index]=counts
		counts=counts*100
		counts=np.around(counts, decimals=2)
		result={"counts":counts,"description":row['description'],"title":row["title"],"url":row["video_id"]}
		final_return.append(result)	
	cur.close()
	conn.close()

	return final_return

@app.route('/plot1.png')
def plot1_png():
	fig = create_figure(list(final_range[0]))
	output = io.BytesIO()
	FigureCanvas(fig).print_png(output)
	return Response(output.getvalue(), mimetype='image/png')


@app.route('/plot2.png')
def plot2_png():
	fig = create_figure(list(final_range[1]))
	output = io.BytesIO()
	FigureCanvas(fig).print_png(output)
	return Response(output.getvalue(), mimetype='image/png')


@app.route('/plot3.png')
def plot3_png():
	fig = create_figure(list(final_range[2]))
	output = io.BytesIO()
	FigureCanvas(fig).print_png(output)
	return Response(output.getvalue(), mimetype='image/png')


@app.route('/plot4.png')
def plot4_png():
	fig = create_figure(list(final_range[3]))
	output = io.BytesIO()
	FigureCanvas(fig).print_png(output)
	return Response(output.getvalue(), mimetype='image/png')

@app.route('/plot5.png')
def plot5_png():
	fig = create_figure(list(final_range[4]))
	output = io.BytesIO()
	FigureCanvas(fig).print_png(output)
	return Response(output.getvalue(), mimetype='image/png')

def create_figure(value):
	value=list(value)
	fig = Figure(figsize=(6,3.5))
	axis = fig.add_subplot(1, 1, 1)
	category=['Happy','Information','Not satisfied','Question','Request']
	
	axis.barh(category,value)
	for s in ['top', 'bottom', 'left', 'right']:
		axis.spines[s].set_visible(False)
    
	axis.set_title("Sentiment Analytics")

	# Remove x, y Ticks
	axis.xaxis.set_ticks_position('none')
	axis.yaxis.set_ticks_position('none')
	
	# Add padding between axes and labels
	axis.xaxis.set_tick_params(pad = 5)
	axis.yaxis.set_tick_params(pad = 0)
	
	# Add x, y gridlines
	axis.grid(visible = True, color ='grey',
			linestyle ='-.', linewidth = 0.5,
			alpha = 0.2)
	
	# Show top values
	axis.invert_yaxis()
	
	# Add annotation to bars
	for i in axis.patches:
		plt.text(i.get_width()+0.2, i.get_y()+0.5,
				str(round((i.get_width()), 2)),
				fontsize = 10, fontweight ='bold',
				color ='grey')
	

	# Adding the labels
	axis.set_ylabel("Category")
	axis.set_xlabel("Range")
	axis.set_xlim([0,1])
	fig.tight_layout()
	return fig


@app.route('/', methods=["POST","GET"])
def search_results():
	if request.method=="POST":
		query=request.form['query'] 
		result = youtube_search_keyword(query)
		return render_template('index.html',result=result)
	if request.method=="GET":

		result=youtube_search_keyword('CSE')
		print(result)
		return render_template('index.html',result=result)

if __name__=='__main__':
	app.run(debug=True)