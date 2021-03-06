import json
import io 
from httpretty import HTTPretty, httprettified
import pytumblr 
from urlparse import parse_qs
import sqlite3
import sys
from urlparse import urlparse
import numpy
import pygraphviz as pgv

def init():
	try:
		# SQLite
		con = sqlite3.connect('tumblr.s3db')
		cur = con.cursor()
		
		# Uncomment at first run
		#cur.execute("DROP TABLE IF EXISTS Cars")
		#cur.execute("CREATE TABLE reblogs(reblog_key TEXT, source_url TEXT, blog TEXT)")
		#cur.execute("CREATE TABLE queue(blog_url TEXT, visited BOOLEAN )")
		#con.commit()
		
		# Tumblr Client
		client = pytumblr.TumblrRestClient(
		'2gisMO8el52yyLvZ1nVTtYhJwnKU8kpQCAKJziv3pKOqVCJ0mG',
		'O98yMGNKgXq5uKPQwKVevnWE9EgijXQR6pjZqgAtnRUeYJ8w8K',
		'AgVORhqu6d9WAEF8GrwXEfSpmC4wsPu1MVN4jWJfCGfkahQJm0',
		'3Psj6lPRdplQdeYfe8CgKzl9CEgFvrZlWrfm1LHZXg2dD7YEXW',
		)

		run(con,cur,client)
		#rank_ReflectionAlgorithm(con,cur,client)

	except sqlite3.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	finally:
		if con:
			con.commit()
			con.close()

# Take a blog, fetch N last post and find their sources (if they are reblogged).
# Then add them to a queue.
# Do this steps for all the items in the queue.
def run(con,cur,client):
	
	while 1:
		# Take first item in queue
		cur.execute('SELECT blog_url FROM queue WHERE visited=0 limit(1)')
		data = cur.fetchone()
		
		if data==None:
			break
			
		url = data[0]
		
		print 'blog',#url,
		posts = client.posts(url,limit=20)
		
		if 'blog' in posts:
			print '[Fetched]',
			
			# Uncomment if you want to sampling from blog
			#posts = client.posts('underweartuesday.tumblr.com',limit=1)
			#sample_rate = 0.1; # percent
			#total_post = posts['total_posts']
			#sample_post = total_post * sample_rate;
			#posts = client.posts('underweartuesday.tumblr.com',limit=sample_post)
			
			for post in posts['posts']:
				try:
					src_url = None

					# If tt isn't reblogged post then set itself as source
					if not 'source_url' in post:
						src_url = posts['blog']['url']
					else:
						src_url = post['source_url']

					# Add to reblogs table
					
					src_url = urlparse(src_url)[1]
					#item = (post['reblog_key'],src_url,posts['blog']['url'])
					#item = (post['reblog_key'],src_url,post['link_url'])
					item = (post['reblog_key'],src_url,url)
					cur.execute('INSERT INTO reblogs (reblog_key,source_url,blog) VALUES (?,?,?)',item)

					# Add to queue if it dosn't exist
					cur.execute('SELECT count(*) FROM queue WHERE blog_url=?', (src_url,) )
					if cur.fetchone()[0] == 0:
						cur.execute('INSERT INTO queue (blog_url,visited) VALUES (?,?)',(src_url,0))
					print '.',
				except:
					print 'x',

		# Mark as visited
		cur.execute('UPDATE queue SET visited=1 WHERE blog_url=?', (url,) )
		print '[Marked]'
		con.commit()
		
def rank_ReflectionAlgorithm(con,cur,client):

	blogs_list = []
	G=pgv.AGraph()

	#
	print 'Creating blogs list ...',

	cur.execute("SELECT * FROM queue WHERE visited=1")

	while 1:
		data = cur.fetchone()
		if data == None:	break
		blogs_list.append( data[0].lower() )

	print len(blogs_list)

	# 
	print 'Creating matrix ...',

	dim = len(blogs_list)
	blog_src_matrix = numpy.zeros((dim, dim ),int)
	
	print '[OK]'

	# 
	print 'Filling matrix ...',

	cur.execute("SELECT count(*) FROM reblogs")
	total = cur.fetchone()

	cur.execute("SELECT * FROM reblogs")

	j = i = 0
	while 1:
		data = cur.fetchone()
		if data == None:	break

		src_url = data[1].lower()
		blog_url = data[2].lower()
		#src_url = urlparse(src_url)[0]
		blog_url = urlparse(blog_url)[1];

		#print src_url, blog_url,

		try:
			src_index = blogs_list.index(src_url)
			blog_index = blogs_list.index(blog_url)
			#print src_index,blog_index,src_index
			
			G.add_edge(src_index,blog_index)

			blog_src_matrix[blog_index,src_index] = blog_src_matrix[blog_index,src_index] + 1
		except:
			j = j+1

		i = i+1
		#print i,'/',total,' - ',j

	print '[OK]'

	#
	#numpy.savetxt("blog_src_matrix.csv", blog_src_matrix, delimiter=",")

	# 
	fav_url = raw_input("Enter your fac url:")
	fav_blog_index = blogs_list.index(urlparse(fav_url)[1]);
	sources_vector = blog_src_matrix[fav_blog_index,:]

	similarity_vector = blog_src_matrix.dot(sources_vector)

	# Top 10 suggetion

	for i in range(1,10):
		maxIdx = -1
		for idx in range(0,dim):
			if 	idx!=fav_blog_index and ( maxIdx==-1 or similarity_vector[idx] > similarity_vector[maxIdx] ):
				maxIdx = idx

		print i, blogs_list[maxIdx], similarity_vector[maxIdx]
		similarity_vector[maxIdx] = -1

	G.write('graph.dot')
	G.layout()
	G.draw('visualized.png')

	#for idx in range(0,dim):
	#	if blog_src_matrix[fav_blog_index,idx] != 0:
	#		print blogs_list[idx]

if __name__ == '__main__':
	init()
