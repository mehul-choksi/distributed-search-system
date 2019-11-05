import Pyro4
import time
import os
import asyncio

def callback(obj, value):
	print(str(value))

class Master(object):
	pool = None
	
	def __init__(self):
		self.slave_uri_list = []
	def connect(self):
		#read all slave uris
		with open('config','r') as reader:
			for line in reader:
				self.slave_uri_list.append(line.strip())
		print(self.slave_uri_list)

	def search(self, query):
		all_responses = []	
	
		for uri in self.slave_uri_list:
			curr_slave = Pyro4.Proxy(uri)
			all_responses.append(curr_slave.slave_query(query))
		print(all_responses)	
		
		
	"""async def search_a(self, query, uri):
		slave = Pyro4.Proxy(uri)
		resp = slave.slave_query(query)
		print(resp)

	async def main():
		await asyncio.gather(self.search_a(query,'PYRO:obj_a05fdbde13784b19b2a610d110d58b34@localhost:17450'), 
		)
	"""

	async def search_a(self, query,uri):
		slave = Pyro4.Proxy(uri)
		#resp = slave.do_async_task(query)
		slave.fast_slave_query(query)		
				
		#print("resp from "+uri)
		#print(resp)
		#print("******************************")
		return slave.get_response()
		
	async def main(self, query):
		
		function_list = []
		for uri in self.slave_uri_list:
			function_list.append(self.search_a(query,uri))
		results = await asyncio.gather(*function_list )
		print(results)
		# return results

	def doSearch(self, query):
		asyncio.run(self.main(query))


	def cleanup(self):
		for uri in self.slave_uri_list:
			curr_slave = Pyro4.Proxy(uri)
			curr_slave.shutdown()

master = Master()
master.connect()

try:
	while True:
		#time.sleep(2)
		query = input('Enter query: ').strip()
		master.doSearch(query)
except KeyboardInterrupt:
	writer = open('config', 'w')
	writer.close()
	master.cleanup()
	
