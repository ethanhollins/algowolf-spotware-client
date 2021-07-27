import sys
import socketio
import os
import json
import traceback
import time
from app.spotware import Spotware
from app.db import Database


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

'''
Utilities
'''
class UserContainer(object):

	def __init__(self, sio, db, config):
		self.sio = sio
		self.db = db
		self.config = config
		self.parent = None
		self.users = {}


	def getSio(self):
		return self.sio


	def getConfig(self):
		return self.config


	def setParent(self, parent):
		self.parent = parent


	def getParent(self):
		return self.parent


	def addUser(self, user_id, broker_id, access_token, refresh_token, accounts, is_parent, is_dummy):
		if broker_id not in self.users:
			self.users[broker_id] = Spotware(
				self, user_id, broker_id, access_token=access_token, refresh_token=refresh_token, 
				accounts=accounts, is_parent=is_parent, is_dummy=is_dummy
			)
			if is_parent:
				self.parent = self.users[broker_id]

		else:
			self.users[broker_id].setVars(user_id, broker_id, access_token, refresh_token, accounts, is_parent, is_dummy)

		return self.users[broker_id]


	def deleteUser(self, broker_id):
		if broker_id in self.users:
			del self.users[broker_id]


	def getUser(self, broker_id):
		return self.users.get(broker_id)


def getConfig():
	path = os.path.join(ROOT_DIR, 'instance/config.json')
	if os.path.exists(path):
		with open(path, 'r') as f:
			return json.load(f)
	else:
		raise Exception('Config file does not exist.')


'''
Initialize
'''

config = getConfig()
sio = socketio.Client()
db = Database(config)
user_container = UserContainer(sio, db, config)

'''
Socket IO functions
'''

def sendResponse(msg_id, res):
	res = {
		'msg_id': msg_id,
		'result': res
	}

	sio.emit(
		'broker_res', 
		res, 
		namespace='/broker'
	)


def onAddUser(user_id, broker_id, access_token, refresh_token, accounts, is_parent=False, is_dummy=False):
	print('[onAddUser] 1', flush=True)
	user = user_container.addUser(
		user_id, broker_id, access_token, refresh_token, accounts, is_parent=is_parent, is_dummy=is_dummy
	)
	print('[onAddUser] 2', flush=True)

	if access_token is not None:
		print('[onAddUser] 3', flush=True)
		user.start()

	# if is_dummy:
	# 	getParent().deleteChild(user)

	print('[onAddUser] 4', flush=True)
	if user.is_auth:
		print('[onAddUser] 5', flush=True)
		return {
			'access_token': user.access_token,
			'refresh_token': user.refresh_token
		}

	else:
		print('[onAddUser] 6', flush=True)
		return {
			'error': 'Not Authorised'
		}


def onDeleteUser(broker_id):
	user_container.deleteUser(broker_id)

	return {
		'completed': True
	}


def getUser(broker_id):
	return user_container.getUser(broker_id)


def getParent():
	return user_container.getParent()


def getUserTokens(broker_id):
	user = getUser(broker_id)

	if user and user.is_auth:
		return {
			'access_token': user.access_token,
			'refresh_token': user.refresh_token
		}

	else:
		return {
			'error': 'Not Authorised'
		}


# Download Historical Data EPT
def _download_historical_data_broker( 
	user, product, period, tz='Europe/London', 
	start=None, end=None, count=None,
	include_current=True,
	**kwargs
):
	return user._download_historical_data_broker(
		product, period, tz='Europe/London', 
		start=start, end=end, count=count,
		**kwargs
	)


def _subscribe_chart_updates(user, msg_id, instrument):
	user._subscribe_chart_updates(msg_id, instrument)

	return {
		'completed': True
	}


def onSwDisconnect():
	while True:
		try:
			if not user_container.getParent().demo_client.is_connected:
				user_container.getParent().demo_client.connect()
		except Exception:
			print(traceback.format_exc(), flush=True)

		try:
			if not user_container.getParent().live_client.is_connected:
				user_container.getParent().live_client.connect()
		except Exception:
			print(traceback.format_exc(), flush=True)

		time.sleep(1)


@sio.on('connect', namespace='/broker')
def onConnect():
	print('CONNECTED!', flush=True)


@sio.on('disconnect', namespace='/broker')
def onDisconnect():
	print('DISCONNECTED', flush=True)


@sio.on('broker_cmd', namespace='/broker')
def onCommand(data):
	print(f'COMMAND: {data}', flush=True)

	try:
		cmd = data.get('cmd')
		broker = data.get('broker')
		broker_id = data.get('broker_id')

		if broker_id is None:
			user = getParent()
		else:
			user = getUser(broker_id)

		if broker == 'spotware':
			res = {}
			if cmd == 'add_user':
				res = onAddUser(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'get_tokens':
				res = getUserTokens(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'delete_user':
				res = onDeleteUser(*data.get('args'), **data.get('kwargs'))

			elif cmd == '_download_historical_data_broker':
				res = _download_historical_data_broker(user, *data.get('args'), **data.get('kwargs'))

			elif cmd == '_subscribe_chart_updates':
				res = _subscribe_chart_updates(user, *data.get('args'), **data.get('kwargs'))

			elif cmd == '_subscribe_account_updates':
				res = user._subscribe_account_updates(*data.get('args'), **data.get('kwargs'))

			elif cmd == '_get_all_positions':
				res = user._get_all_positions(*data.get('args'), **data.get('kwargs'))

			elif cmd == '_get_all_orders':
				res = user._get_all_orders(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'createPosition':
				res = user.createPosition(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'modifyPosition':
				res = user.modifyPosition(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'deletePosition':
				res = user.deletePosition(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'getAllAccounts':
				res = user.getAllAccounts(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'getAccountInfo':
				res = user.getAccountInfo(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'createOrder':
				res = user.createOrder(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'modifyOrder':
				res = user.modifyOrder(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'deleteOrder':
				res = user.deleteOrder(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'deleteChild':
				res = getParent().deleteChild(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'checkAccessToken':
				res = getParent().checkAccessToken(*data.get('args'), **data.get('kwargs'))

			sendResponse(data.get('msg_id'), res)

	except Exception as e:
		print(traceback.format_exc(), flush=True)
		sendResponse(data.get('msg_id'), {
			'error': str(e)
		})


def createApp():
	print('CREATING APP', flush=True)
	onAddUser("PARENT", "PARENT", None, None, None, is_parent=True, is_dummy=False)
	print('CREATING APP DONE', flush=True)

	while True:
		try:
			sio.connect(
				config['STREAM_URL'], 
				headers={
					'Broker': 'spotware'
				}, 
				namespaces=['/broker']
			)
			break
		except Exception:
			pass

	# PARENT_USER_CONFIG = config['PARENT_USER']
	# parent = FXCM(**PARENT_USER_CONFIG)
	# user_container.setParent(parent)

	return sio


if __name__ == '__main__':
	sio = createApp()
	print('DONE', flush=True)

	onSwDisconnect()
