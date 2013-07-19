import json
import io 
from httpretty import HTTPretty, httprettified
import pytumblr 
from urlparse import parse_qs
import sqlite3
import sys
from urlparse import urlparse

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

		run(cur,client)

	except sqlite3.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	finally:
		if con:
			con.commit()
			con.close()

def run(cur,client):
	
	while 1:
		cur.execute('SELECT blog_url FROM queue WHERE visited=0 limit(1)')
		data = cur.fetchone()
		
		if data==None:
			break
			
		url = data[0]
		
		print url,
		posts = client.posts(url,limit=20)
		
		if 'blog' in posts:
			print '[Fetched]',
			#posts = client.posts('underweartuesday.tumblr.com',limit=1)
			#sample_rate = 0.1; # percent
			#total_post = posts['total_posts']
			#sample_post = total_post * sample_rate;
			#posts = client.posts('underweartuesday.tumblr.com',limit=sample_post)
			
			for post in posts['posts']:
				src_url = None

				if not 'source_url' in post: # It isn't reblogged post
					src_url = posts['blog']['url']
				else:
					src_url = post['source_url']

				src_url = urlparse(src_url)[1]

				item = (post['reblog_key'],src_url,posts['blog']['url'])
				cur.execute('INSERT INTO reblogs (reblog_key,source_url,blog) VALUES (?,?,?)',item)

				# Add to queue if it dosn't exist
				cur.execute('SELECT count(*) FROM queue WHERE blog_url=?', (src_url,) )
				if cur.fetchone()[0] == 0:
					cur.execute('INSERT INTO queue (blog_url,visited) VALUES (?,?)',(src_url,0))
				
				print '.',

		# Mark as visited
		cur.execute('UPDATE queue SET visited=1 WHERE blog_url=?', (url,) )
		print '[Marked]'

if __name__ == '__main__':
	init()