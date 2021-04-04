import os
import socket
import ssl
import traceback
import time
from threading import Thread
from struct import pack, unpack
from .messages import OpenApiCommonMessages_pb2 as o1
from .messages import OpenApiMessages_pb2 as o2


CONNECT_EVENT = 'connect'
DISCONNECT_EVENT = 'disconnect'
MESSAGE_EVENT = 'message'

class Client(object):

	def __init__(self, broker, is_demo=False, timeout=None):
		self._events = {
			CONNECT_EVENT: [],
			DISCONNECT_EVENT: [],
			MESSAGE_EVENT: []
		}

		self.broker = broker
		self.is_demo = is_demo
		self.host = "demo.ctraderapi.com" if is_demo else "live.ctraderapi.com"
		self.port = 5035

		self.ssock = None

		self._populate_protos()

		Thread(target=self._perform_send).start()



	def _populate_protos(self):
		self._protos = {}
		for name in dir(o1) + dir(o2):
			if not name.startswith("Proto"):
				continue

			m = o1 if hasattr(o1, name) else o2
			klass = getattr(m, name)
			self._protos[klass().payloadType] = klass


	def connect(self, timeout=None):
		if self.ssock is not None:
			try:
				self.ssock.close()
			except Exception:
				pass

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		if timeout:
			sock.settimeout(timeout)

		PEM_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "cert.pem")
		self.ssock = ssl.wrap_socket(sock, ssl_version=ssl.PROTOCOL_TLS, certfile=PEM_PATH, keyfile=PEM_PATH)
		self.ssock.connect((self.host, self.port))

		print('[SC] Connected', flush=True)

		t = Thread(target=self.receive)
		t.start()

		try:
			for e in self._events[CONNECT_EVENT]:
				e(self.is_demo)
		except Exception:
			print(traceback.format_exc(), flush=True)


	def reconnect(self):
		while True:
			print('[SC] Attempting reconnect.', flush=True)
			try:
				self.connect()
				break
			except Exception:
				time.sleep(1)


	def _perform_send(self):

		while True:
			try:
				if len(self.broker._msg_queue) and self.broker._msg_queue[0][0] == self.is_demo:
					_, payload, msgid = self.broker._msg_queue[0]
					del self.broker._msg_queue[0]

					if self.ssock is not None:
						proto_msg = o1.ProtoMessage(
							payloadType=payload.payloadType,
							payload=payload.SerializeToString(),
							clientMsgId=msgid
						).SerializeToString()

						sock_msg = pack("!I", len(proto_msg)) + proto_msg
						# print(f'[SC] SEND MSG: {sock_msg}')
						self.ssock.send(sock_msg)

					else:
						print('[SC] Not connected.')

					self.broker.req_count += 1

				if self.broker.req_count >= 50:
					time.sleep(1)
					self.broker.req_timer = time.time()
					self.broker.req_count = 0

				elif time.time() - self.broker.req_timer > 1:
					self.broker.req_timer = time.time()
					self.broker.req_count = 0

			except Exception:
				print(f'[SC] SEND ERROR:\n{traceback.format_exc()}', flush=True)

			time.sleep(0.001)


	def send(self, payload, msgid=''):
		self.broker._msg_queue.append((self.is_demo, payload, msgid))


	def receive(self):
		buffer=b''
		msg_len = 0
		msg = b''

		while True:
			try:
				recv = self.ssock.recv(1024)
				if len(recv) == 0:
					break
				buffer += recv
			except Exception:
				print(f'[SC] ERROR:\n{traceback.format_exc()}', flush=True)
				break

			while len(buffer):
				# print(buffer)
				# Retrieve message length
				if msg_len == 0:
					if len(buffer) >= 4:
						msg_len = unpack("!I", buffer[:4])[0]
						buffer = buffer[4:]
					else:
						break

				# Retrieve message
				if msg_len > 0:
					# Get new message
					new_message = buffer[:msg_len]
					# Delete read info from buffer
					buffer = buffer[min(len(buffer), msg_len):]
					# Update msg length remaining
					msg_len -= len(new_message)
					# Add to result message
					msg += new_message

					if msg_len == 0:
						# Message callback
						proto_msg = o1.ProtoMessage()
						proto_msg.ParseFromString(msg)
						payload = self._protos[proto_msg.payloadType]()
						payload.ParseFromString(proto_msg.payload)
						try:
							for e in self._events[MESSAGE_EVENT]:
								Thread(target=e, args=(self.is_demo, proto_msg.payloadType, payload, proto_msg.clientMsgId)).start()
								# e(self.is_demo, proto_msg.payloadType, payload, proto_msg.clientMsgId)
						except Exception:
							print(traceback.format_exc(), flush=True)


						msg = b''

		print('[SC] Disconnected...', flush=True)
		try:
			for e in self._events[DISCONNECT_EVENT]:
				e(self.is_demo)
		except Exception:
			print(traceback.format_exc(), flush=True)

		self.reconnect()


	def stop(self):
		self.ssock.close()


	def event(self, event_type, func):
		if callable(func):
			if event_type == CONNECT_EVENT:
				self._events[CONNECT_EVENT].append(func)

			elif event_type == DISCONNECT_EVENT:
				self._events[DISCONNECT_EVENT].append(func)

			elif event_type == MESSAGE_EVENT:
				self._events[MESSAGE_EVENT].append(func)

