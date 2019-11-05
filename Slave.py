import Pyro4
import traceback
import os
from os.path import *
import mmap
import sys
import re
import threading
@Pyro4.expose
class Slave(object):
	slave_files = []
	slave_daemon = None

	def __init__(self):
		self.response = {}
	def setup(self, dir_name):
		daemon = Pyro4.Daemon()
		Slave.slave_daemon = daemon
		uri = str(daemon.register(Slave))
		writer = open('config', 'a')
		print("Uri = " + str(uri))
		writer.write(str(uri) + '\n')
		writer.close()
		for entry in os.listdir(dir_name):
			print(entry, end = " ")
			if(isfile('./' + dir_name + '/' + entry)):
				Slave.slave_files.append('./' + dir_name + '/' + entry)
		print(Slave.slave_files)
		print("Slave running... " + str(uri))
		daemon.requestLoop()
	"""
	async def async_slave_query(self,query):
		print("[Async Mode]: Query = " + query)

		slave_files = Slave.slave_files
		todo = []		
		for curr_file in slave_files:
			todo.append(self.slave_search(curr_file,query))
		await asyncio.gather(*todo)

	async def main(self,query):
		asyncio.run(self.async_slave_query(query))
	
		return self.response

	"""
	def fast_slave_query(self,query):
		print("[Multithreading Mode]: Query = " + query)

		slave_files = Slave.slave_files
		thread_list = []		
		for curr_file in slave_files:
			thread = threading.Thread(target=self.slave_search(curr_file,query))
			thread_list.append(thread)
			thread.start()
		for thread in thread_list:
			thread.join()

	def get_response(self):
		return self.response

	def slave_query(self, query):
		print("Query = " + query)
		
		slave_files = Slave.slave_files
		for curr_file in slave_files:
			#response.append(self.slave_search(curr_file,query))	
			self.slave_search(curr_file,query)
		return self.response		
		#return "query recieved:" + self.uri
	
	def slave_search(self,file_name,search_key):
		
		match_count = 0
		matches = []
		with open(file_name, 'r+') as f:
			if(os.stat(file_name).st_size == 0):
				return
			with mmap.mmap(f.fileno(), 0,access=mmap.ACCESS_READ) as m:
				search_res = re.finditer(str.encode(search_key),m)
				for res in search_res:
					matches.append(res.span()[0])
					match_count = match_count+1
		
		self.response[file_name.split('/')[-1]] = (match_count, matches)
		#print(matches)
		#return matches
	def shutdown(self):
		Slave.slave_daemon.shutdown()
try:
	dir_name = sys.argv[1]
	slave = Slave()
	slave.setup(dir_name)
except:
	traceback.print_exc()
