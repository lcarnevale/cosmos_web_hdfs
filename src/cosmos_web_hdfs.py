import requests

class CosmosWebHDFS():

	def __init__(self, username, url_compute, url_storage, port_compute=13000, port_storage=14000, version='v1'):
		self.__username = username
		self.__port_compute = port_compute
		self.__port_storage = port_storage
		self.__url_compute = url_compute
		self.__url_storage = url_storage
		self.__version = version


	def set_token(self, username, password):
		url = "%s:%s/cosmos-auth/%s/token" % (self.__url_compute, self.__port_compute, self.__version)

		headers = {
			'Content-Type': 'application/x-www-form-urlencoded'
		}

		payload = {
			'grant_type': 'password',
			'username': username,
			'password': password
		}

		r = requests.post(
				url, 
				data=payload, 
				headers=headers, 
				verify=False
			)
		
		self.__token = r.json()['access_token']
		return True

	def get_token(self):
		return self.__token


	def mkdir(self, path):
		url = "%s:%s/webhdfs/%s/user/%s/%s?op=MKDIRS" % (self.__url_storage, self.__port_storage, self.__version,self.__username, path)
		
		headers = {
			'X-Auth-token': self.__token,
		}

		params = {
			'user.name': self.__username,
		}

		r = requests.put(
				url, 
				params=params,
				headers=headers, 
			)
		
		return r.json()['boolean']
	
	def liststatus(self, path):
		url = "%s:%s/webhdfs/%s/user/%s/%s?op=LISTSTATUS" % (self.__url_storage, self.__port_storage, self.__version, self.__username, path)

		headers = {
			'X-Auth-token': self.__token,
		}

		params = {
			'user.name': self.__username,
		}


		r = requests.get(
				url, 
				params=params,
				headers=headers
			)

		return r.json()

	def create_file(self, path, payload):
		url = "%s:%s/webhdfs/%s/user/%s/%s?op=CREATE" % (self.__url_storage, self.__port_storage, self.__version, self.__username, path)

		headers = {
			'X-Auth-token': self.__token,
			'Content-Type': 'application/octet-stream'
		}

		params = {
			'user.name': self.__username,
		}

		r = requests.put(url,params=params,headers=headers,data=payload)

		try:
			if r.json()['RemoteException']['exception']:
				return r.json()['RemoteException']['exception']
		except ValueError:
			return True

	def open_file(self, path):
		url = "%s:%s/webhdfs/%s/user/%s/%s?op=OPEN" % (self.__url_storage, self.__port_storage, self.__version, self.__username, path)

		headers = {
			'X-Auth-token': self.__token,
		}

		params = {
			'user.name': self.__username,
		}


		r = requests.get(
				url, 
				params=params,
				headers=headers
			)

		return r.text

	def delete_file(self, path):
		url = "%s:%s/webhdfs/%s/user/%s/%s?op=DELETE" % (self.__url_storage, self.__port_storage, self.__version, self.__username, path)

		headers = {
			'X-Auth-token': self.__token,
		}

		params = {
			'user.name': self.__username,
		}


		r = requests.delete(
				url, 
				params=params,
				headers=headers
			)

		return r.json()['boolean']

if __name__ == '__main__':
	whdfs = CosmosWebHDFS(
				username='INSERT_HERE', 
				url_compute='INSERT_HERE',
				url_storage='INSERT_HERE'
			)

	whdfs.set_token('INSERT_HERE', 'INSERT_HERE')
	print whdfs.mkdir('mydir')
	print whdfs.liststatus('mydir')
	print whdfs.create_file('mydir/file.txt', "luke,tatooine,jedi\nleila,tatooine,politician")
	print whdfs.liststatus('mydir/file.txt')
	print whdfs.open_file('mydir/file.txt')
	print whdfs.delete_file('mydir/file.txt')

