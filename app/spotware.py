import time
import traceback
import numpy as np
import pandas as pd
import json
import ntplib
import shortuuid
from copy import copy
from threading import Thread
from datetime import datetime
from google.protobuf.json_format import MessageToDict
from .spotware_connect.client import Client
from .spotware_connect.messages import OpenApiCommonMessages_pb2 as o1
from .spotware_connect.messages import OpenApiMessages_pb2 as o2
from app import tradelib as tl

ONE_HOUR = 60*60


class ChartSubscription(object):

	def __init__(self, broker, msg_id):
		self.broker = broker
		self.msg_id = msg_id


	def onChartUpdate(self, payload):
		# args = [payload.ask, payload.bid]
		args = [MessageToDict(payload)]

		# self.broker._send_response(
		# 	self.msg_id,
		# 	{
		# 		'args': args,
		# 		'kwargs': {}
		# 	}
		# )

		self.broker.container.send_queue.append({
			"type": "price",
			"message": {
				'msg_id': self.msg_id,
				'result': {
					'args': args,
					'kwargs': {}
				}
			}
		})

		# self.broker.container.zmq_req_socket.recv()


class AccountSubscription(object):

	def __init__(self, broker, msg_id):
		self.broker = broker
		self.msg_id = msg_id


	def onAccountUpdate(self, *args, **kwargs):
		args = list(args)

		print(f'ACCOUNT UPDATE: {args}, {kwargs}')
		# self.broker._send_response(
		# 	self.msg_id,
		# 	{
		# 		'args': args,
		# 		'kwargs': kwargs
		# 	}
		# )

		self.broker.container.send_queue.append({
			"type": "account",
			"message": {
				'msg_id': self.msg_id,
				'result': {
					'args': args,
					'kwargs': kwargs
				}
			}
		})

		# self.broker.zmq_req_socket.recv()



class Spotware(object):

	def __init__(self,
		container, user_id, strategy_id, broker_id, access_token=None, refresh_token=None, 
		accounts={}, is_parent=False, is_dummy=False
	):
		self.container = container
		self.userId = user_id
		self.strategyId = strategy_id
		self.brokerId = broker_id
		self.accounts = accounts
		self.is_parent = is_parent
		self.is_dummy = is_dummy

		self._spotware_connected = False
		self._first_auth = False
		self._last_update = time.time()
		self.is_running = True
		self._subscriptions = {}
		self._chart_subscriptions = []
		self._account_subscriptions = []
		self._account_update_queue = []
		self._handled = {}
		self._handled_position_events = {
			tl.MARKET_ENTRY: {},
			tl.POSITION_CLOSE: {}
		}
		self._msg_queue = []
		self.req_timer = time.time()
		self.req_count = 0
		# self.assets = assets
		# self.symbols = symbols

		self.assets = {}
		self.assets_by_name = {}
		self.symbols = {}
		self.symbols_by_name = {}

		self.access_token = access_token
		self.refresh_token = refresh_token

		self.is_auth = False
		self.initialized = False

		# self.time_off = 0
		# self._set_time_off()

		Thread(target=self.accountUpdateLoop).start()

		'''
		Setup Spotware Funcs
		'''
		if self.is_parent:
			self.parent = self
			self.children = []

			self.demo_client = Client(self, True)
			self.live_client = Client(self, False)

			self.demo_client.event('connect', self.connect)
			self.demo_client.event('disconnect', self.disconnect)
			self.demo_client.event('message', self.message)

			self.live_client.event('connect', self.connect)
			self.live_client.event('disconnect', self.disconnect)
			self.live_client.event('message', self.message)

			self.demo_client.connect()
			self.live_client.connect()

			# while not self._spotware_connected:
			# 	pass
		else:
			self.parent = self.container.getParent()
			self.parent.addChild(self)


	def setVars(self, user_id, strategy_id, broker_id, access_token=None, refresh_token=None, 
		accounts={}, is_parent=False, is_dummy=False):
		self.userId = user_id
		self.strategyId = strategy_id
		self.brokerId = broker_id
		self.accounts = accounts
		self.is_parent = is_parent
		self.is_dummy = is_dummy

		self.access_token = access_token
		self.refresh_token = refresh_token

	
	def start(self):
		self.initialized = True
		if self.is_parent:
			self.is_auth = self._authorize_accounts(self.accounts, is_parent=True)

		else:
			# self.client = self.parent.client
			self.is_auth = self._authorize_accounts(self.accounts, is_dummy=self.is_dummy)

		# Start refresh thread
		Thread(target=self._periodic_refresh).start()

	def _set_time_off(self):
		try:
			client = ntplib.NTPClient()
			response = client.request('pool.ntp.org')
			self.time_off = response.tx_time - time.time()
		except Exception:
			pass


	def generateReference(self):
		return shortuuid.uuid()


	'''
	Spotware messages
	'''

	def _periodic_refresh(self):
		TWO_SECONDS = 2
		while self.is_running:
			if self.is_parent:
				if time.time() - self._last_update > TWO_SECONDS:
					try:
						heartbeat = o1.ProtoHeartbeatEvent()
						self.demo_client.send(heartbeat)
						self.live_client.send(heartbeat)
						self._last_update = time.time()
					except Exception as e:
						print(f'[SC] {str(e)}', flush=True)
						pass
			else:
				if time.time() - self._last_update > 30:
					try:
						self._last_update = time.time()
						for account_id in self.accounts:
							ref_id = self.generateReference()
							pos_req = o2.ProtoOAReconcileReq(
								ctidTraderAccountId=int(account_id)
							)
							self._get_client(account_id).send(pos_req, msgid=ref_id)
							self.parent._wait(ref_id)
					except Exception as e:
						print(f'[SC] {str(e)}', flush=True)
						pass

			time.sleep(1)


	def heartbeat(self):
		return {
			"initialized": self.initialized
		}


	def _wait(self, ref_id, polling=0.1, timeout=30):
		start = time.time()
		while not ref_id in self._handled:
			if time.time() - start >= timeout:
				return None
			time.sleep(polling)

		item = self._handled[ref_id]
		del self._handled[ref_id]
		return item


	def _wait_for_position(self, order_id, polling=0.1, timeout=30):
		start = time.time()
		while not order_id in self._handled_position_events[tl.MARKET_ENTRY]:
			if time.time() - start >= timeout:
				return None
			time.sleep(polling)

		return self._handled_position_events[tl.MARKET_ENTRY][order_id]


	def _wait_for_close(self, order_id, polling=0.1, timeout=30):
		start = time.time()
		while not order_id in self._handled_position_events[tl.POSITION_CLOSE]:
			if time.time() - start >= timeout:
				return None
			time.sleep(polling)

		return self._handled_position_events[tl.POSITION_CLOSE][order_id]


	def _get_client(self, account_id):
		if self.accounts[str(account_id)]['is_demo']:
			return self.parent.demo_client
		else:
			return self.parent.live_client


	def _is_demo(self, account_id):
		if self.accounts[str(account_id)]['is_demo']:
			return True
		else:
			return False


	# def _send_response(self, msg_id, res):
	# 	res = {
	# 		'msg_id': msg_id,
	# 		'result': res
	# 	}

	# 	self.container.sio.emit(
	# 		'broker_res', 
	# 		res, 
	# 		namespace='/broker'
	# 	)



	def connect(self, is_demo):
		print('Spotware connected!', flush=True)

		# Application Auth
		auth_req = o2.ProtoOAApplicationAuthReq(
			clientId = self.container.getConfig()['SPOTWARE_CLIENT_ID'],
			clientSecret = self.container.getConfig()['SPOTWARE_CLIENT_SECRET']
		)
		if is_demo:
			self.demo_client.send(auth_req, msgid=self.generateReference())
		else:
			self.live_client.send(auth_req, msgid=self.generateReference())


	def disconnect(self, is_demo):
		print('Spotware disconnected', flush=True)


	def message(self, is_demo, payloadType, payload, msgid):

		if not payloadType in (2138,2131,2165,2120,2153,2160,2113,2115):
			print(f'MSG: ({payloadType}) {payload}', flush=True)

		# Heartbeat
		if payloadType == 51:
			heartbeat = o1.ProtoHeartbeatEvent()
			if is_demo:
				self.demo_client.send(heartbeat)
			else:
				self.live_client.send(heartbeat)
			self._last_update = time.time()

		elif payloadType == 2101:
			self.parent._spotware_connected = True

			print(f'RE AUTH ACCOUNTS: {is_demo} {self.parent.children}', flush=True)

			# Wait for both clients to be connected before reconnecting
			start_time = time.time()
			if is_demo:
				while not self.parent.demo_client.is_connected:
					if time.time() - start_time > 30:
						return
					time.sleep(1)
			else:
				while not self.parent.live_client.is_connected:
					if time.time() - start_time > 30:
						return
					time.sleep(1)

			if self.parent.accounts is not None:
				self.parent.is_auth = self.parent._authorize_accounts(self.parent.accounts, is_parent=True, check_is_demo=is_demo)
				
			for child in self.parent.children:
				print(child.accounts, flush=True)
				child._authorize_accounts(child.accounts, check_is_demo=is_demo)
				# for sub in child._account_subscriptions:
					# sub.onAccountUpdate(None, None, {'type': 'connected'}, None)
				child._account_update_queue.append((None, None, {'type': 'connected'}, None))

			self.parent._resubscribe_chart_updates()

		# Tick
		elif payloadType == 2131:
			if str(payload.symbolId) in self._subscriptions:
				self._subscriptions[str(payload.symbolId)].onChartUpdate(payload)

		else:
			if payloadType == 2142 and payload.errorCode == "SERVER_IS_UNDER_MAINTENANCE":
				self._spotware_connected = True
			elif payloadType == 2142 and payload.errorCode == "CANT_ROUTE_REQUEST":
				self._spotware_connected = False

			result = None
			if 'ctidTraderAccountId' in payload.DESCRIPTOR.fields_by_name.keys():
				# print(f'MSG: {payload}')
				account_id = payload.ctidTraderAccountId
				for child in copy(self.parent.children):
					if account_id in map(int, child.accounts.keys()):
						# for sub in child._account_subscriptions:
							# sub.onAccountUpdate(payloadType, account_id, MessageToDict(payload), msgid)
						child._account_update_queue.append((payloadType, account_id, MessageToDict(payload), msgid))

						# if isinstance(result, dict): 
						# 	for k, v in result.items():
						# 		if v['accepted']:
						# 			if v['type'] == tl.MARKET_ENTRY:
						# 				self._handled_position_events[tl.MARKET_ENTRY][v['item'].order_id] = {k: v}
						# 			elif v['type'] == tl.POSITION_CLOSE:
						# 				self._handled_position_events[tl.POSITION_CLOSE][v['item'].order_id] = {k: v}

						break

			if msgid:
				if result is None:
					self._handled[msgid] = payload
				else:
					self._handled[msgid] = result


	def _set_options(self):
		path = self.ctrl.app.config['BROKERS']
		with open(path, 'r') as f:
			options = json.load(f)
		
		options[self.name] = {
			**options[self.name],
			**{
				"access_token": self.access_token,
				"refresh_token": self.refresh_token
			}
		}

		with open(path, 'w') as f:
			f.write(json.dumps(options, indent=2))


	def _refresh_token(self, is_parent=False):
		print(f'REFRESH: (A) {self.access_token} (R) {self.refresh_token}', flush=True)

		ref_id = self.generateReference()
		refresh_req = o2.ProtoOARefreshTokenReq(
			refreshToken=self.refresh_token
		)
		self.parent.demo_client.send(refresh_req, msgid=ref_id)
		res = self.parent._wait(ref_id)
		if res.payloadType == 2174:
			self.access_token = res.accessToken
			self.refresh_token = res.refreshToken
			print(f'NEW: (A) {self.access_token} (R) {self.refresh_token}', flush=True)
			if is_parent:
				if self.container.getConfig()['SERVER'] == 0:
					self.container.db.updateUser(
						'spotware',
						{
							'access_token': self.access_token,
							'refresh_token': self.refresh_token
						}
					)
				elif self.container.getConfig()['SERVER'] == 1:
					self.container.db.updateUser(
						'spotware_1',
						{
							'access_token': self.access_token,
							'refresh_token': self.refresh_token
						}
					)

			else:
				self.container.db.updateBroker(
					self.userId, self.brokerId, 
					{ 
						'access_token': self.access_token,
						'refresh_token': self.refresh_token
					}
				)

			return True
		else:
			return False


	def _authorize_accounts(self, accounts, is_parent=False, is_dummy=False, check_is_demo=None):
		print(f'MSG: {self.brokerId}, {accounts}', flush=True)

		if not is_dummy and self.refresh_token is not None:
			is_auth = self._refresh_token(is_parent=is_parent)
			if not is_auth:
				return False

		is_auth = True
		for account_id in accounts:
			if check_is_demo is not None:
				if (self._is_demo(account_id) and not check_is_demo or
					not self._is_demo(account_id) and check_is_demo):
					continue


			ref_id = self.generateReference()
			acc_auth = o2.ProtoOAAccountAuthReq(
				ctidTraderAccountId=int(account_id), 
				accessToken=self.access_token
			)
			self._get_client(account_id).send(acc_auth, msgid=ref_id)
			res = self.parent._wait(ref_id)
			
			is_auth = False
			if res.payloadType == 2142:
				if res.errorCode == "ALREADY_LOGGED_IN":
					is_auth = True
				else:
					is_auth = False
			else:
				is_auth = True
			
			if is_auth:
				trader_ref_id = self.generateReference()
				trader_req = o2.ProtoOATraderReq(
					ctidTraderAccountId=int(account_id)
				)
				self._get_client(account_id).send(trader_req, msgid=trader_ref_id)
				trader_res = self.parent._wait(trader_ref_id)

				self._set_broker_info(account_id, trader_res.trader.brokerName)

		return is_auth


	def _set_broker_info(self, account_id, broker_name):
		if broker_name not in self.parent.assets or broker_name not in self.parent.symbols:
			print(f'Setting {broker_name} info...', flush=True)

			asset_ref_id = self.generateReference()
			asset_req = o2.ProtoOAAssetListReq(
				ctidTraderAccountId=int(account_id)
			)
			self._get_client(account_id).send(asset_req, msgid=asset_ref_id)
			asset_res = self.parent._wait(asset_ref_id)

			self.parent.assets[broker_name] = {}
			self.parent.assets_by_name[broker_name] = {}
			for i in asset_res.asset:
				self.parent.assets[broker_name][str(i.assetId)] = {
					'name': i.name,
					'displayName': i.displayName
				}
				self.parent.assets_by_name[broker_name][str(i.name)] = {
					'assetId': i.assetId,
					'displayName': i.displayName
				}

			symbol_ref_id = self.generateReference()
			symbol_req = o2.ProtoOASymbolsListReq(
				ctidTraderAccountId=int(account_id) 
			)
			self._get_client(account_id).send(symbol_req, msgid=symbol_ref_id)
			symbol_res = self.parent._wait(symbol_ref_id)

			self.parent.symbols[broker_name] = {}
			self.parent.symbols_by_name[broker_name] = {}
			for i in symbol_res.symbol:
				self.parent.symbols[broker_name][str(i.symbolId)] = {
					'symbolName': i.symbolName,
					'baseAssetId': i.baseAssetId,
					'quoteAssetId': i.quoteAssetId,
					'symbolCategoryId': i.symbolCategoryId
				}
				self.parent.symbols_by_name[broker_name][str(i.symbolName)] = {
					'symbolId': i.symbolId,
					'baseAssetId': i.baseAssetId,
					'quoteAssetId': i.quoteAssetId,
					'symbolCategoryId': i.symbolCategoryId
				}

	def authCheck(self):
		return {'result': self.is_auth}


	'''
	Broker functions
	'''

	def _download_historical_data_broker(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		sw_product = self._convert_product('Spotware', product)
		sw_period = self._convert_period(period)

		result = pd.concat((
			self._create_empty_asks_df(), 
			self._create_empty_mids_df(), 
			self._create_empty_bids_df()
		))

		dl_start = None
		dl_end = None
		if start:
			# dl_start = tl.utils.convertTimeToTimestamp(start)
			dl_start = start
		if end:
			# dl_end = tl.utils.convertTimeToTimestamp(end)
			dl_end = end

		if count:
			if start:
				start = tl.utils.convertTimestampToTime(start)
				dl_end = tl.utils.convertTimeToTimestamp(tl.utils.getCountDate(period, count+1, start=start))
			elif end:
				end = tl.utils.convertTimestampToTime(end)
				dl_start = tl.utils.convertTimeToTimestamp(tl.utils.getCountDate(period, count+1, end=end))
			else:
				dl_start = tl.utils.convertTimeToTimestamp(tl.utils.getCountDate(period, count+1))
				dl_end = tl.utils.convertTimeToTimestamp(datetime.utcnow()) + tl.period.getPeriodOffsetSeconds(period)

		while True:
			ref_id = self.generateReference()
			trendbars_req = o2.ProtoOAGetTrendbarsReq(
				ctidTraderAccountId=int(list(self.accounts.keys())[0]),
				fromTimestamp=int(dl_start*1000), toTimestamp=int(dl_end*1000), 
				symbolId=sw_product, period=sw_period
			)
			self._get_client(list(self.accounts.keys())[0]).send(trendbars_req, msgid=ref_id)

			res = self._wait(ref_id)

			'''
			Bar Constructor
			'''

			if res.payloadType == 2138:
				mids = self._bar_data_constructor(res, self._create_empty_mids_df())
				asks = mids.copy()
				asks.columns = ['ask_open', 'ask_high', 'ask_low', 'ask_close']
				bids = mids.copy()
				bids.columns = ['bid_open', 'bid_high', 'bid_low', 'bid_close']

				result = pd.concat((
					result,
					pd.concat((asks, mids, bids), axis=1)
				))

				if count and result.shape[0] < count:
					dl_end = dl_start
					dl_start = tl.convertTimeToTimestamp(tl.utils.getCountDate(
						period, count+1, end=tl.convertTimestampToTime(dl_end)
					))
				else:
					break

			else:
				print(f'ERROR: {res}')

		return result.to_dict()


	def convert_sw_position(self, account_id, pos):
		order_id = str(pos['positionId'])
		product = self._convert_sw_product(int(pos['tradeData']['symbolId']))
		direction = tl.LONG if pos['tradeData']['tradeSide'] == 'BUY' else tl.SHORT
		lotsize = self._convert_from_sw_lotsize(float(pos['tradeData']['volume']))
		entry_price = float(pos['price'])
		sl = None if pos.get('stopLoss') is None or float(pos['stopLoss']) == 0 else round(float(pos['stopLoss']), 5)
		tp = None if pos.get('takeProfit') is None or float(pos['takeProfit']) == 0 else round(float(pos['takeProfit']), 5)
		open_time = float(pos['tradeData']['openTimestamp']) / 1000

		return tl.Position(
			self,
			order_id, str(account_id), product,
			tl.MARKET_ENTRY, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def convert_sw_order(self, account_id, order):
		entry_price = None
		if order['orderType'] == 'STOP':
			entry_price = float(order['stopPrice'])
			order_type = tl.STOP_ORDER
		elif order['orderType'] == 'LIMIT':
			entry_price = float(order['limitPrice'])
			order_type = tl.LIMIT_ORDER

		order_id = str(order['orderId'])
		product = self._convert_sw_product(int(order['tradeData']['symbolId']))
		direction = tl.LONG if order['tradeData']['tradeSide'] == 'BUY' else tl.SHORT
		lotsize = self._convert_from_sw_lotsize(float(order['tradeData']['volume']))

		if order.get('stopLoss') is not None:
			sl = round(float(order['stopLoss']), 5)
		elif order.get('relativeStopLoss') is not None:
			if direction == tl.LONG:
				sl = round(entry_price - tl.utils.convertToPrice(float(order.get('relativeStopLoss'))/10), 5)
			else:
				sl = round(entry_price + tl.utils.convertToPrice(float(order.get('relativeStopLoss'))/10), 5)
		else:
			sl = None

		if order.get('takeProfit') is not None:
			tp = round(float(order['takeProfit']), 5)
		elif order.get('relativeTakeProfit') is not None:
			if direction == tl.LONG:
				tp = round(entry_price + tl.utils.convertToPrice(float(order.get('relativeTakeProfit'))/10), 5)
			else:
				tp = round(entry_price - tl.utils.convertToPrice(float(order.get('relativeTakeProfit'))/10), 5)
		else:
			tp = None

		open_time = float(order['tradeData']['openTimestamp']) / 1000

		return tl.Order(
			self,
			order_id, str(account_id), product,
			order_type, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def _get_all_positions(self, account_id):
		ref_id = self.generateReference()
		pos_req = o2.ProtoOAReconcileReq(
			ctidTraderAccountId=int(account_id)
		)
		self._get_client(account_id).send(pos_req, msgid=ref_id)
		res = self.parent._wait(ref_id)
		print(f"[_get_all_positions] {res}", flush=True)

		result = { account_id: [] }
		if res.payloadType == 2125:
			for pos in res.position:
				new_pos = self.convert_sw_position(account_id, MessageToDict(pos))

				result[account_id].append(new_pos)

		return result


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_tp_prices, sl_tp_ranges
	):
		ref_id = self.generateReference()

		broker_name = self.accounts[account_id]['broker']
		sw_product = self._convert_product(broker_name, product)
		direction = 1 if direction == tl.LONG else 2
		lotsize = self._convert_to_sw_lotsize(lotsize)

		'''
		TEMP
		'''
		# sl_tp_ranges['relativeStopLoss'] = int(round(sl_tp_ranges['relativeStopLoss']/100) * 100)
		# sl_tp_ranges['relativeTakeProfit'] = int(round(sl_tp_ranges['relativeTakeProfit']/100) * 100)
		# lotsize = int(lotsize / 100000)
		'''
		TEMP
		'''
		start_time = time.time()
		print(f'CREATE POSITION START: {self.brokerId}', flush=True)

		# Execute Market Order
		new_order = o2.ProtoOANewOrderReq(
			ctidTraderAccountId=int(account_id),
			symbolId=sw_product, orderType=1, tradeSide=direction,
			volume=lotsize, **sl_tp_ranges
		)
		print(f'Sending:\n{new_order}', flush=True)
		self._get_client(account_id).send(new_order, msgid=ref_id)
		res = self.parent._wait(ref_id)
		print(f'Result:\n{res}', flush=True)
		print(f'CREATE POSITION END: {self.brokerId} {round(time.time() - start_time, 2)}s', flush=True)

		result = {}
		if res.payloadType == 2126:
			# new_pos = self.convert_sw_position(account_id, res.position)
			result = {
				'order_id': str(res.position.positionId)
			}
			# pos_res = self.parent._wait_for_position(str(res.position.positionId))
			# print(f'Pos Res: {pos_res}', flush=True)

			# if pos_res is not None:
			# 	ref_id = list(pos_res.keys())[0]
			# 	item = pos_res[ref_id]
			# 	pos = item['item']

			# 	if len(sl_tp_prices) > 0:
			# 		mod_ref_id = self.generateReference()

			# 		amend_req = o2.ProtoOAAmendPositionSLTPReq(
			# 			ctidTraderAccountId=int(pos.account_id),
			# 			positionId=int(pos.order_id), **sl_tp_prices
			# 		)
			# 		self._get_client(account_id).send(amend_req, msgid=mod_ref_id)

			# 		res = self.parent._wait(mod_ref_id)

			# 	result.update(pos_res)

		elif not res is None and res.payloadType in (50, 2132):
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.MARKET_ENTRY,
					'accepted': False,
					'message': res.errorCode
				}
			})

		else:
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.MARKET_ENTRY,
					'accepted': False
				}
			})

		return result


	def modifyPosition(self, account_id, order_id, sl_price, tp_price):
		ref_id = self.generateReference()

		start_time = time.time()
		print(f'MODIFY POSITION START: {self.brokerId}', flush=True)
		amend_req = o2.ProtoOAAmendPositionSLTPReq(
			ctidTraderAccountId=int(account_id),
			positionId=int(order_id), stopLoss=sl_price, takeProfit=tp_price
		)
		self._get_client(account_id).send(amend_req, msgid=ref_id)

		res = self.parent._wait(ref_id)
		print(f'MODIFY POSITION END: {self.brokerId} {round(time.time() - start_time, 2)}s', flush=True)
		
		if res is not None and res.payloadType in (50, 2132):
			res = {
				ref_id: {
					'timestamp': time.time(),
					'type': tl.MODIFY,
					'accepted': False,
					'message': res.errorCode
				}
			}
		else:
			res = {
				'ref_id': ref_id
			}

		return res


	def deletePosition(self, account_id, order_id, lotsize):
		ref_id = self.generateReference()

		lotsize = self._convert_to_sw_lotsize(lotsize)

		close_req = o2.ProtoOAClosePositionReq(
			ctidTraderAccountId=int(account_id),
			positionId=int(order_id), volume=lotsize
		)
		self._get_client(account_id).send(close_req, msgid=ref_id)

		start_time = time.time()
		print(f'DELETE POSITION START: {self.brokerId}', flush=True)
		res = self.parent._wait(ref_id)
		print(f'DELETE POSITION END: {self.brokerId} {round(time.time() - start_time, 2)}s', flush=True)

		# Handle delete result
		result = {}
		if res.payloadType == 2126:
			result = {
				'order_id': str(order_id)
			}
			# pos_res = self.parent._wait_for_close(str(order_id))
			# if not pos_res is None:
			# 	ref_id = list(pos_res.keys())[0]
			# 	item = pos_res[ref_id]
			# 	print(f'Pos Res [delete]: {pos_res}', flush=True)

			# 	result.update(pos_res)

		elif not res is None and res.payloadType in (50, 2132):
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.POSITION_CLOSE,
					'accepted': False,
					'message': res.errorCode
				}
			})

		else:
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.POSITION_CLOSE,
					'accepted': False
				}
			})

		return result


	def _get_all_orders(self, account_id):
		ref_id = self.generateReference()
		order_req = o2.ProtoOAReconcileReq(
			ctidTraderAccountId=int(account_id)
		)
		self._get_client(account_id).send(order_req, msgid=ref_id)
		res = self.parent._wait(ref_id)

		result = { account_id: [] }
		if res.payloadType == 2125:
			for order in res.order:
				new_order = self.convert_sw_order(account_id, MessageToDict(order))

				result[account_id].append(new_order)

		return result


	def getAllAccounts(self):
		print(f'ALL ACOUNTS: {self.access_token}')

		ref_id = self.generateReference()
		accounts_req = o2.ProtoOAGetAccountListByAccessTokenReq(
			accessToken=self.access_token
		)
		self.parent.demo_client.send(accounts_req, msgid=ref_id)

		res = self.parent._wait(ref_id)
		if res is not None and res.payloadType != 2142:
			self.accounts = { str(i.ctidTraderAccountId): { 'is_demo': not i.isLive } for i in res.ctidTraderAccount }
			self._authorize_accounts([i.ctidTraderAccountId for i in res.ctidTraderAccount])

			result = []
			for i in res.ctidTraderAccount:
				if res.permissionScope == 1:
					trader_ref_id = self.generateReference()
					trader_req = o2.ProtoOATraderReq(
						ctidTraderAccountId=int(i.ctidTraderAccountId)
					)

					self._get_client(i.ctidTraderAccountId).send(trader_req, msgid=trader_ref_id)

					trader_res = self.parent._wait(trader_ref_id)

					result.append({
						'id': i.ctidTraderAccountId,
						'is_demo': not i.isLive,
						'account_id': i.traderLogin,
						'broker': trader_res.trader.brokerName
					})
				else:
					return None

			return result

		else:
			return None


	def checkAccessToken(self, access_token):
		ref_id = self.generateReference()
		accounts_req = o2.ProtoOAGetAccountListByAccessTokenReq(
			accessToken=access_token
		)
		self.parent.demo_client.send(accounts_req, msgid=ref_id)

		res = self.parent._wait(ref_id)
		if res is not None:
			return MessageToDict(res)
		else:
			return {
				'error': 'failed'
			}


	def getAccountInfo(self, account_id):
		ref_id = self.generateReference()
		trader_req = o2.ProtoOATraderReq(
			ctidTraderAccountId=int(account_id)
		)
		self._get_client(account_id).send(trader_req, msgid=ref_id)
		res = self.parent._wait(ref_id)

		# Handle account info result

		result = {}

		currency = self._get_asset(res.trader.brokerName, res.trader.depositAssetId)['name']
		balance = res.trader.balance/100

		print(f'INFO: {currency}, {balance}', flush=True)
		if res.payloadType == 2122:
			result[account_id] = {
				'currency': currency,
				'balance': balance,
				'pl': None,
				'margin': None,
				'available': None
			}
		
		return result


	def createOrder(self, 
		product, lotsize, direction,
		account_id, order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		ref_id = self.generateReference()

		# Convert symbol
		# symbol_id = 

		params = {}
		if order_type == tl.STOP_ORDER:
			params['stopPrice'] = entry_price
		elif order_type == tl.LIMIT_ORDER:
			params['limitPrice'] = entry_price

		if sl_price:
			params['stopLoss'] = sl_price
		else:
			params['relativeStopLoss'] = sl_range

		if tp_price:
			params['takeProfit'] = tp_price
		else:
			params['relativeTakeProfit'] = tp_range

		direction = 1 if direction == tl.LONG else 2
		sw_order_type = 3 if order_type == tl.STOP_ORDER else 2
		# lotsize = round(lotsize / 1000000) * 1000000
		lotsize = self._convert_to_sw_lotsize(lotsize)

		'''
		TEMP
		'''
		# lotsize = int(lotsize / 100000)
		'''
		TEMP
		'''

		start_time = time.time()
		print(f'CREATE ORDER START: {self.brokerId}, {self.accounts}', flush=True)
		broker_name = self.accounts[account_id]['broker']
		new_order_req = o2.ProtoOANewOrderReq(
			ctidTraderAccountId=int(account_id),
			symbolId=self._convert_product(broker_name, product), orderType=sw_order_type, tradeSide=direction,
			volume=lotsize, **params
		)
		self._get_client(account_id).send(new_order_req, msgid=ref_id)

		res = self.parent._wait(ref_id)
		print(f'CREATE ORDER END: {self.brokerId} {round(time.time() - start_time, 2)}s', flush=True)

		if res is not None and res.payloadType in (50, 2132):
			res = {
				ref_id: {
					'timestamp': time.time(),
					'type': order_type,
					'accepted': False,
					'message': res.errorCode
				}
			}
		else:
			res = {
				'ref_id': ref_id
			}

		return res


	def modifyOrder(self, account_id, order_id, order_type, lotsize, entry_price, sl_price, tp_price, override=False):
		ref_id = self.generateReference()

		args = {}
		if not entry_price is None:
			if order_type == tl.STOP_ORDER:
				args['stopPrice'] = entry_price
			elif order_type == tl.LIMIT_ORDER:
				args['limitPrice'] = entry_price
		if not lotsize is None:
			'''
			TEMP
			'''
			# lotsize = int(lotsize / 100000)
			'''
			TEMP
			'''

			args['volume'] = self._convert_to_sw_lotsize(lotsize)
		if not sl_price is None:
			args['stopLoss'] = sl_price
		if not tp_price is None:
			args['takeProfit'] = tp_price

		start_time = time.time()
		print(f'MODIFY ORDER START: {self.brokerId}', flush=True)
		amend_req = o2.ProtoOAAmendOrderReq(
			ctidTraderAccountId=int(account_id), orderId=int(order_id),
			**args
		)
		self._get_client(account_id).send(amend_req, msgid=ref_id)
		res = self.parent._wait(ref_id)
		print(f'MODIFY ORDER END: {self.brokerId} {round(time.time() - start_time, 2)}s', flush=True)

		if res is not None and res.payloadType in (50, 2132):
			res = {
				ref_id: {
					'timestamp': time.time(),
					'type': tl.MODIFY,
					'accepted': False,
					'message': res.errorCode
				}
			}

		else:
			res = {
				'ref_id': ref_id
			}

		# print(f'MOD: {res}', flush=True)

		return res


	def deleteOrder(self, account_id, order_id, override=False):
		ref_id = self.generateReference()

		start_time = time.time()
		print(f'DELETE ORDER START: {self.brokerId}', flush=True)
		cancel_req = o2.ProtoOACancelOrderReq(
			ctidTraderAccountId=int(account_id), orderId=int(order_id)
		)
		self._get_client(account_id).send(cancel_req, msgid=ref_id)
		print(f'DELETE ORDER END: {self.brokerId} {round(time.time() - start_time, 2)}s', flush=True)

		

		res = self.parent._wait(ref_id)

		if res is not None and res.payloadType in (50, 2132):
			res = {
				ref_id: {
					'timestamp': time.time(),
					'type': tl.ORDER_CANCEL,
					'accepted': False,
					'message': res.errorCode
				}
			}

		else:
			res = {
				'ref_id': ref_id
			}

		return res


	def _subscribe_chart_updates(self, msg_id, instrument):
		subscription = ChartSubscription(self, msg_id)

		ref_id = self.generateReference()

		instrument = self._convert_product('Spotware', instrument)
		self.parent._subscriptions[str(instrument)] = subscription

		sub_req = o2.ProtoOASubscribeSpotsReq(
			ctidTraderAccountId=int(list(self.accounts.keys())[0]),
			symbolId=[instrument]
		)
		self._get_client(list(self.accounts.keys())[0]).send(sub_req, msgid=ref_id)
		self.parent._wait(ref_id)

		# sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
		# 	ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 	symbolId=product, period=1
		# )
		# self._get_client(list(self.accounts.keys())[0]).send(sub_req)

		for i in range(14):
			if i % 5 == 0:
				time.sleep(1)

			sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
				ctidTraderAccountId=int(list(self.accounts.keys())[0]),
				symbolId=instrument, period=i+1
			)
			self._get_client(list(self.accounts.keys())[0]).send(sub_req)


	def _resubscribe_chart_updates(self):
		for instrument in self.parent._subscriptions:
			ref_id = self.generateReference()
			
			sub_req = o2.ProtoOASubscribeSpotsReq(
				ctidTraderAccountId=int(list(self.accounts.keys())[0]),
				symbolId=[int(instrument)]
			)
			self._get_client(list(self.accounts.keys())[0]).send(sub_req, msgid=ref_id)
			self.parent._wait(ref_id)

			# sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
			# 	ctidTraderAccountId=int(list(self.accounts.keys())[0]),
			# 	symbolId=product, period=1
			# )
			# self._get_client(list(self.accounts.keys())[0]).send(sub_req)

			for i in range(14):
				if i % 5 == 0:
					time.sleep(1)

				sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
					ctidTraderAccountId=int(list(self.accounts.keys())[0]),
					symbolId=int(instrument), period=i+1
				)
				self._get_client(list(self.accounts.keys())[0]).send(sub_req)


	def _subscribe_multiple_chart_updates(self, products, listener):
		ref_id = self.generateReference()

		products = [self._convert_product(i) for i in products]
		for product in products:
			self.parent._subscriptions[str(product)] = listener

		sub_req = o2.ProtoOASubscribeSpotsReq(
			ctidTraderAccountId=int(list(self.accounts.keys())[0]),
			symbolId=products
		)
		self._get_client(list(self.accounts.keys())[0]).send(sub_req, msgid=ref_id)
		self.parent._wait(ref_id)

		# sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
		# 	ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 	symbolId=products, period=1
		# )
		# self._get_client(list(self.accounts.keys())[0]).send(sub_req)

		for i in range(14):
			if i % 5 == 0:
				time.sleep(1)

			sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
				ctidTraderAccountId=int(list(self.accounts.keys())[0]),
				symbolId=products, period=i+1
			)
			self.client.send(sub_req)


	def _get_account_subscription_msg_id(self):
		for sub in self._account_subscriptions:
			return sub.msg_id
		return None


	def _subscribe_account_updates(self, msg_id):
		existing_msg_id = self._get_account_subscription_msg_id()

		if existing_msg_id is None:
			subscription = AccountSubscription(self, msg_id)
			self._account_subscriptions.append(subscription)
		else:
			msg_id = existing_msg_id
		
		return msg_id


	def onChartUpdate(self, chart, payload):
		# self._price_queue.append((chart, payload))
		return

	def getBrokerKey(self):
		return self.strategyId + '.' + self.brokerId

	def getDbPositions(self):
		positions = self.container.redis_client.hget(self.getBrokerKey(), "positions")
		if positions is None:
			positions = []
		else:
			positions = json.loads(positions)
		return positions

	def setDbPositions(self, positions):
		self.container.redis_client.hset(self.getBrokerKey(), "positions", json.dumps(positions))

	def appendDbPosition(self, new_position):
		positions = self.getDbPositions()
		positions.append(new_position)
		self.setDbPositions(positions)

	def deleteDbPosition(self, order_id):
		positions = self.getDbPositions()
		for i in range(len(positions)):
			if positions[i]["order_id"] == order_id:
				del positions[i]
				break
		self.setDbPositions(positions)

	def replaceDbPosition(self, position):
		positions = self.getDbPositions()
		for i in range(len(positions)):
			if positions[i]["order_id"] == position["order_id"]:
				positions[i] = position
				break
		self.setDbPositions(positions)
	
	def getDbOrders(self):
		orders = self.container.redis_client.hget(self.getBrokerKey(), "orders")
		if orders is None:
			orders = []
		else:
			orders = json.loads(orders)
		return orders

	def setDbOrders(self, orders):
		self.container.redis_client.hset(self.getBrokerKey(), "orders", json.dumps(orders))

	def appendDbOrder(self, new_order):
		orders = self.getDbOrders()
		orders.append(new_order)
		self.setDbOrders(orders)

	def deleteDbOrder(self, order_id):
		orders = self.getDbOrders()
		for i in range(len(orders)):
			if orders[i]["order_id"] == order_id:
				del orders[i]
				break
		self.setDbOrders(orders)

	def replaceDbOrder(self, order):
		orders = self.getDbOrders()
		for i in range(len(orders)):
			if orders[i]["order_id"] == order["order_id"]:
				orders[i] = order
				break
		self.setDbOrders(orders)


	def accountUpdateLoop(self):
		while True:
			if len(self._account_update_queue):
				try:
					payload_type, account_id, update, ref_id = self._account_update_queue[0]
					self._handle_account_update(payload_type, account_id, update, ref_id)
				except Exception:
					print(traceback.format_exc())
				finally:
					del self._account_update_queue[0]


	def _handle_account_update(self, payload_type, account_id, update, msg_id):

		if update.get('type') == 'connected':
			print(f'[_on_account_update] RECONNECTED')
			if self.userAccount and self.brokerId:
				print(f'[_on_account_update] Retrieving positions/orders')
				# self._handle_live_strategy_setup()

		elif payload_type is not None:
			if int(payload_type) == 2125:
				update_positions = []
				update_orders = []
				for pos in update.get("position", []):
					new_pos = self.convert_sw_position(account_id, pos)
					update_positions.append(new_pos)
				for order in update.get("order", []):
					new_order = self.convert_sw_order(account_id, order)
					update_orders.append(new_order)
				positions = [i for i in self.getDbPositions() if str(i["account_id"]) != str(account_id)]
				positions += update_positions
				orders = [i for i in self.getDbOrders() if str(i["account_id"]) != str(account_id)]
				orders += update_orders

				self.setDbPositions(positions)
				self.setDbOrders(orders)

			elif int(payload_type) == 2126:
				print(f'_on_account_update: {payload_type} {account_id} {update}', flush=True)
				# if not ref_id:
				ref_id = self.generateReference()

				print(f'Account Update: {update}')
				execution_type = update['executionType']

				result = {}
				position_event = None
				# ORDER_FILLED
				# if execution_type in ('ORDER_FILLED', 'ORDER_PARTIAL_FILL'):
				if execution_type == 'ORDER_FILLED':
					# Check `closingOrder`
					if update['order']['closingOrder']:
						# Delete
						positions = self.getDbPositions()
						for i in range(len(positions)):
							pos = positions[i]
							if str(update['position']['positionId']) == pos["order_id"]:
								if update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
									pos["close_price"] = float(update['order']['executionPrice'])
									pos["close_time"] = float(update['order']['utcLastUpdateTimestamp']) / 1000

									self.deleteDbPosition(pos["order_id"])

									if update['order'].get('limitPrice'):
										tp_dist = abs(float(update['order']['executionPrice']) - float(update['order']['limitPrice']))
									else:
										tp_dist = None

									if update['order'].get('stopPrice'):
										sl_dist = abs(float(update['order']['executionPrice']) - float(update['order']['stopPrice']))
									else:
										sl_dist = None

									if sl_dist is None:
										order_type = tl.TAKE_PROFIT
									elif tp_dist is None:
										order_type = tl.STOP_LOSS
									elif tp_dist < sl_dist:
										order_type = tl.TAKE_PROFIT
									else:
										order_type = tl.STOP_LOSS

									result.update({
										ref_id: {
											'timestamp': pos["close_time"],
											'type': order_type,
											'accepted': True,
											'item': pos
										}
									})
								else:
									# Fully Closed
									if float(update['position']['tradeData']['volume']) == 0:
										pos["close_price"] = float(update['order']['executionPrice'])
										pos["close_time"] = float(update['order']['utcLastUpdateTimestamp']) / 1000

										self.deleteDbPosition(pos["order_id"])

										result.update({
											ref_id: {
												'timestamp': pos["close_time"],
												'type': tl.POSITION_CLOSE,
												'accepted': True,
												'item': pos
											}
										})

										# self._handled_position_events[tl.POSITION_CLOSE][pos["order_id"]] = {
										# 	ref_id: {
										# 		'timestamp': pos["close_time"],
										# 		'type': tl.POSITION_CLOSE,
										# 		'accepted': True,
										# 		'item': pos
										# 	}
										# }
										position_event = tl.POSITION_CLOSE

									# Partially Closed
									else:
										pos["lotsize"] -= self._convert_from_sw_lotsize(float(update['order']['executedVolume']))

										self.replaceDbPosition(pos)

										del_pos = tl.Position.fromDict(self, pos)
										del_pos.lotsize = self._convert_from_sw_lotsize(float(update['order']['executedVolume']))
										del_pos.close_price = float(update['order']['executionPrice'])
										del_pos.close_time = float(update['order']['utcLastUpdateTimestamp']) / 1000

										result.update({
											ref_id: {
												'timestamp': del_pos.close_time,
												'type': tl.POSITION_CLOSE,
												'accepted': True,
												'item': del_pos
											}
										})

										# self._handled_position_events[tl.POSITION_CLOSE][del_pos.order_id] = {
										# 	ref_id: {
										# 		'timestamp': del_pos.close_time,
										# 		'type': tl.POSITION_CLOSE,
										# 		'accepted': True,
										# 		'item': del_pos
										# 	}
										# }
										position_event = tl.POSITION_CLOSE

								break
					else:
						pos_order = None
						order_type = tl.MARKET_ENTRY
						orders = self.getDbOrders()
						for i in range(len(orders)):
							order = orders[i]
							if str(update['order']['orderId']) == order["order_id"]:
								pos_order = order
								order["close_time"] = float(update['order']['utcLastUpdateTimestamp']) / 1000
								if order["order_type"] == tl.STOP_ORDER:
									order_type = tl.STOP_ENTRY
								elif order["order_type"] == tl.LIMIT_ORDER:
									order_type = tl.LIMIT_ENTRY
									
								self.deleteDbOrder(order["order_id"])

								for sub in self._account_subscriptions:
									sub.onAccountUpdate(
										account_id,
										{
											self.generateReference(): {
												'timestamp': order["close_time"],
												'type': tl.ORDER_CANCEL,
												'accepted': True,
												'item': order
											}
										}, None, None
									)
								break

						# Create
						new_pos = self.convert_sw_position(account_id, update['position'])
						new_pos.setOrder(pos_order)
						if pos_order is not None:
							new_pos.handled_check = False
						else:
							new_pos.handled_check = True

						print(f"[_handle_account_update] APPENDING...")
						self.appendDbPosition(new_pos)

						result.update({
							ref_id: {
								'timestamp': new_pos.open_time,
								'type': order_type,
								'accepted': True,
								'item': new_pos
							}
						})

						if order_type == tl.MARKET_ENTRY:
							# self._handled_position_events[tl.MARKET_ENTRY][new_pos.order_id] = {
							# 	ref_id: {
							# 		'timestamp': new_pos.open_time,
							# 		'type': order_type,
							# 		'accepted': True,
							# 		'item': new_pos
							# 	}
							# }
							position_event = tl.MARKET_ENTRY


				# ORDER_ACCEPTED
				elif execution_type == 'ORDER_ACCEPTED':
					# Check if `STOP` or `LIMIT`
					if update['order']['orderType'] in ('LIMIT','STOP'):
						new_order = self.convert_sw_order(account_id, update['order'])

						self.appendDbOrder(new_order)

						result.update({
							ref_id: {
								'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
								'type': new_order.order_type,
								'accepted': True,
								'item': new_order
							}
						})

					# Check if `STOP_LOSS_TAKE_PROFIT`
					elif update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
						positions = self.getDbPositions()
						for i in range(len(positions)):
							pos = positions[i]
							if str(update['position']['positionId']) == pos["order_id"]:
								new_sl = None if update['position'].get('stopLoss') is None else round(float(update['position']['stopLoss']), 5)
								new_tp = None if update['position'].get('takeProfit') is None else round(float(update['position']['takeProfit']), 5)

								if not pos["handled_check"] and pos["order"] is not None:
									pos["handled_check"] = True

									if pos["order"]["sl"] != new_sl or pos["order"]["tp"] != new_tp:
										Thread(target=self.position_manager.close, args=(pos,), kwargs={'override': True}).start()
										print(f'ORDER NOT FULFILLED CORRECTLY, CLOSING POSITION: {pos["order_id"]}')
										return

								pos["sl"] = new_sl
								pos["tp"] = new_tp

								self.replaceDbPosition(pos)

								result.update({
									ref_id: {
										'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
										'type': tl.MODIFY,
										'accepted': True,
										'item': pos
									}
								})

								break

				# ORDER_CANCELLED
				elif execution_type == 'ORDER_CANCELLED':
					# Check if `STOP` or `LIMIT`
					if update['order']['orderType'] in ('LIMIT','STOP'):
						# Update current order
						new_order = self.convert_sw_order(account_id, update['order'])
						orders = self.getDbOrders()
						for i in range(len(orders)):
							order = orders[i]
							if str(update['order']['orderId']) == order["order_id"]:
								order["close_time"] = float(update['order']['utcLastUpdateTimestamp']) / 1000

								self.deleteDbOrder(order["order_id"])

								result.update({
									ref_id: {
										'timestamp': order["close_time"],
										'type': tl.ORDER_CANCEL,
										'accepted': True,
										'item': order
									}
								})

								break

					# Check if `STOP_LOSS_TAKE_PROFIT`
					elif update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
						positions = self.getDbPositions()
						for pos in positions:
							if str(update['position']['positionId']) == pos["order_id"]:
								new_sl = None if update['position'].get('stopLoss') is None else float(update['position']['stopLoss'])
								pos["sl"] = new_sl
								new_tp = None if update['position'].get('takeProfit') is None else float(update['position']['takeProfit'])
								pos["tp"] = new_tp

								self.replaceDbPosition(pos)

								result.update({
									ref_id: {
										'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
										'type': tl.MODIFY,
										'accepted': True,
										'item': pos
									}
								})

								break

				# ORDER_REPLACED
				elif execution_type == 'ORDER_REPLACED':
					# Check if `STOP` or `LIMIT`
					if update['order']['orderType'] in ('LIMIT','STOP'):
						# Update current order
						new_order = self.convert_sw_order(account_id, update['order'])
						orders = self.getDbOrders()
						for order in orders:
							if str(update['order']['orderId']) == order["order_id"]:

								self.deleteDbOrder(order["order_id"])
								self.appendDbOrder(new_order)

								result.update({
									ref_id: {
										'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
										'type': tl.MODIFY,
										'accepted': True,
										'item': new_order
									}
								})

					# Check if `STOP_LOSS_TAKE_PROFIT`
					elif update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
						# Update current position
						positions = self.getDbPositions()
						for pos in positions:
							if str(update['position']['positionId']) == pos["order_id"]:
								new_sl = None if update['position'].get('stopLoss') is None else float(update['position']['stopLoss'])
								pos["sl"] = new_sl
								new_tp = None if update['position'].get('takeProfit') is None else float(update['position']['takeProfit'])
								pos["tp"] = new_tp

								self.replaceDbPosition(pos)

								result.update({
									ref_id: {
										'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
										'type': tl.MODIFY,
										'accepted': True,
										'item': pos
									}
								})

				# ORDER_REJECTED
				elif execution_type == 'ORDER_REJECTED':
					error_code = update.get('errorCode')
					print(f'ORDER REJECTED: {error_code}')


					position_id = str(update['position']['positionId'])
					positions = self.getDbPositions()
					for i in range(len(positions)):
						pos = positions[i]
						if pos["order_id"] == position_id:
							self.deleteDbPosition(pos["order_id"])

							pos["close_time"] = time.time()
							result.update({
								ref_id: {
									'timestamp': time.time(),
									'type': tl.POSITION_CLOSE,
									'accepted': True,
									'item': pos
								}
							})

							print(f'REJECTING POSITION: {position_id}')

							break

					order_id = str(update['order']['orderId'])
					orders = self.getDbOrders()
					for i in range(len(orders)):
						order = orders[i]
						if order["order_id"] == order_id:
							self.deleteDbOrder(order["order_id"])

							order["close_time"] = time.time()
							result.update({
								ref_id: {
									'timestamp': time.time(),
									'type': tl.ORDER_CANCEL,
									'accepted': True,
									'item': order
								}
							})

							print(f'REJECTING ORDER: {order_id}')

							break


				if len(result):
					print(f'SEND IT: {result}', flush=True)
					# self._handled[msg_id] = result
					for sub in self._account_subscriptions:
						sub.onAccountUpdate(account_id, result, position_event, msg_id)
					
					# self.handleOnTrade(account_id, result)
				# 	return result
				# else:
				# 	return None



	def _convert_to_sw_lotsize(self, lotsize):
		return round((round(lotsize, 2) * 10000000) / 100000) * 100000


	def _convert_from_sw_lotsize(self, lotsize):
		return round(lotsize / 10000000, 2)


	def _convert_product(self, broker_name, product):
		if product == 'BTC_USD':
			product = 'BTC/USD'

		return int(self._get_symbol_by_name(broker_name, product.replace('_', ''))['symbolId'])


	def _convert_sw_product(self, product):
		if product == 2:
			return tl.product.GBPUSD
		elif product == 1:
			return tl.product.EURUSD


	def _get_asset(self, broker_name, asset_id):
		return self.parent.assets[broker_name][str(asset_id)]


	def _get_asset_by_name(self, broker_name, asset_name):
		return self.parent.assets_by_name[broker_name][asset_name]


	def _get_symbol(self, broker_name, symbol_id):
		return self.parent.symbols[broker_name][str(symbol_id)]


	def _get_symbol_by_name(self, broker_name, symbol_name):
		return self.parent.symbols_by_name[broker_name][symbol_name]


	def isPeriodCompatible(self, period):
		return period in [
			tl.period.ONE_MINUTE, tl.period.TWO_MINUTES,
			tl.period.THREE_MINUTES, tl.period.FOUR_MINUTES,
			tl.period.FIVE_MINUTES, tl.period.TEN_MINUTES,
			tl.period.FIFTEEN_MINUTES, tl.period.THIRTY_MINUTES, 
			tl.period.ONE_HOUR, tl.period.FOUR_HOURS, 
			tl.period.TWELVE_HOURS, tl.period.DAILY, 
			tl.period.WEEKLY, tl.period.MONTHLY
		]


	def _convert_period(self, period):
		if period == tl.period.ONE_MINUTE:
			return 1
		elif period == tl.period.TWO_MINUTES:
			return 2
		elif period == tl.period.THREE_MINUTES:
			return 3
		elif period == tl.period.FOUR_MINUTES:
			return 4
		elif period == tl.period.FIVE_MINUTES:
			return 5
		elif period == tl.period.TEN_MINUTES:
			return 6
		elif period == tl.period.FIFTEEN_MINUTES:
			return 7
		elif period == tl.period.THIRTY_MINUTES:
			return 8
		elif period == tl.period.ONE_HOUR:
			return 9
		elif period == tl.period.FOUR_HOURS:
			return 10
		elif period == tl.period.TWELVE_HOURS:
			return 11
		elif period == tl.period.DAILY:
			return 12
		elif period == tl.period.WEEKLY:
			return 13
		elif period == tl.period.MONTHLY:
			return 14


	def _convert_sw_period(self, period):
		if period == 1:
			return tl.period.ONE_MINUTE
		elif period == 2:
			return tl.period.TWO_MINUTES
		elif period == 3:
			return tl.period.THREE_MINUTES
		elif period == 4:
			return tl.period.FOUR_MINUTES
		elif period == 5:
			return tl.period.FIVE_MINUTES
		elif period == 6:
			return tl.period.TEN_MINUTES
		elif period == 7:
			return tl.period.FIFTEEN_MINUTES
		elif period == 8:
			return tl.period.THIRTY_MINUTES
		elif period == 9:
			return tl.period.ONE_HOUR
		elif period == 10:
			return tl.period.FOUR_HOURS
		elif period == 11:
			return tl.period.TWELVE_HOURS
		elif period == 12:
			return tl.period.DAILY
		elif period == 13:
			return tl.period.WEEKLY
		elif period == 14:
			return tl.period.MONTHLY


	def _create_empty_asks_df(self):
		return pd.DataFrame(columns=[
			'timestamp', 'ask_open', 'ask_high', 'ask_low', 'ask_close'
		]).set_index('timestamp')


	def _create_empty_mids_df(self):
		return pd.DataFrame(columns=[
			'timestamp', 'mid_open', 'mid_high', 'mid_low', 'mid_close'
		]).set_index('timestamp')


	def _create_empty_bids_df(self):
		return pd.DataFrame(columns=[
			'timestamp', 'bid_open', 'bid_high', 'bid_low', 'bid_close'
		]).set_index('timestamp')


	def _bar_data_constructor(self, payload, df):
		if not payload.trendbar is None:
			for i in payload.trendbar:
				df.loc[i.utcTimestampInMinutes * 60] = [
					(i.low + i.deltaOpen) / 100000, # Open
					(i.low + i.deltaHigh) / 100000, # High
					i.low / 100000, # Low
					(i.low + i.deltaClose) / 100000 # Close
				]

		return df.sort_index()


	def _tick_data_constructor(self, period, payload, df):
		offset = tl.period.getPeriodOffsetSeconds(period)

		c_ts = None
		bar_ts = None
		price = None
		ohlc = [None] * 4
		for i in range(len(payload.tickData)):
			tick = payload.tickData[i]

			if i == 0:
				c_ts = tick.timestamp
				price = tick.tick
				ohlc = [price] * 4

				# Get Current Bar Timestamp
				ref_ts = tl.utils.getWeekstartDate(tl.convertTimestampToTime(tick.timestamp/1000)).timestamp()
				bar_ts = (int(c_ts/1000) - (int(c_ts/1000) - ref_ts) % offset) * 1000

			else:
				c_ts += tick.timestamp
				price += tick.tick

			if c_ts < bar_ts:
				df.loc[int(bar_ts/1000)] = ohlc

				ref_ts = tl.utils.getWeekstartDate(tl.convertTimestampToTime(tick.timestamp/1000)).timestamp()
				bar_ts = tl.utils.getPrevTimestamp(period, int(bar_ts/1000), now=int(c_ts/1000)) * 1000

				ohlc = [price] * 4

			if ohlc[1] is None or price > ohlc[1]:
				ohlc[1] = price
			if ohlc[2] is None or price < ohlc[2]:
				ohlc[2] = price

			ohlc[0] = price

		df.values[:] = df.values[:] / 100000
		return df


	def addChild(self, child):
		self.children.append(child)


	def deleteChild(self, broker_id):
		for i in range(len(self.children)):
			child = self.children[i]
			if child.brokerId == broker_id and child.is_dummy:
				print(f'DELETE CHILD {broker_id}', flush=True)
				del self.children[i]
				self.container.deleteUser(broker_id)
				break

		if broker_id in self.container.users:
			del self.container.users[broker_id]


	def disconnectBroker(self):
		self.parent.live_client.disconnect()
		self.parent.demo_client.disconnect()

		return {
			'result': 'success'
		}