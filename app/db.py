import boto3
import jwt
import json, csv, gzip, collections
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


class Database(object):

	def __init__(self, config):
		self.config = config

		self._generate_db()
		if config['ENV'] == 'development':
			self.userTable = self._generate_table('algowolf-users-dev')
		else:
			self.config['SECRET_KEY'] = b'\xfd\xd2\xa3\x9f\xe9\x8d&\xf4\xbf1\x82\xfd7\x96\xa1k[\xc8\xbdmF\xc5\x1d\xf55\xf2 J\xf4\xd9\xa3\x8d'
			self.userTable = self._generate_table('algowolf-users')


	# Dynamo DB
	def _generate_db(self):
		self._db_client = boto3.resource(
			'dynamodb',
			region_name='ap-southeast-2'
		)


	def _generate_table(self, table_name):
		return self._db_client.Table(table_name)


	def _convert_to_decimal(self, row):
		if isinstance(row, dict):
			for k in row:
				row[k] = self._convert_to_decimal(row[k])
		elif (not isinstance(row, str) and
			isinstance(row, collections.Iterable)):
			row = list(row)
			for i in range(len(row)):
				row[i] = self._convert_to_decimal(row[i])
		elif isinstance(row, float):
			return Decimal(str(float(row)))
			
		return row


	def _convert_to_float(self, row):
		if isinstance(row, dict):
			for k in row:
				row[k] = self._convert_to_float(row[k])
		elif (not isinstance(row, str) and
			isinstance(row, collections.Iterable)):
			row = list(row)
			for i in range(len(row)):
				row[i] = self._convert_to_float(row[i])
		elif isinstance(row, Decimal):
			return float(row)

		return row


	def getUser(self, user_id):
		res = self.userTable.get_item(
			Key={ 'user_id': user_id }
		)
		if res.get('Item'):
			return self._convert_to_float(res['Item'])
		else:
			return None


	def updateUser(self, user_id, update):
		update_values = self._convert_to_decimal(
			dict([tuple([':{}'.format(i[0]), i[1]])
					for i in update.items()])
		)

		update_exp = ('set ' + ' '.join(
			['{} = :{},'.format(k, k) for k in update.keys()]
		))[:-1]

		res = self.userTable.update_item(
			Key={
				'user_id': user_id
			},
			UpdateExpression=update_exp,
			ExpressionAttributeValues=update_values,
			ReturnValues="UPDATED_NEW"
		)
		return True


	def updateBroker(self, user_id, broker_id, props):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False

		# Upload new broker info
		if 'brokers' in user and broker_id in user['brokers']:
			prev_broker = jwt.decode(user['brokers'][broker_id], self.config['SECRET_KEY'], algorithms=['HS256'])
			new_broker = { **prev_broker, **props }

			print(f'NEW BROKER: {new_broker}')
			key = jwt.encode(new_broker, self.config['SECRET_KEY'], algorithm='HS256').decode('utf8')
			user['brokers'][broker_id] = key

			# Update changes
			update = { 'brokers': user.get('brokers') }
			result = self.updateUser(user_id, update)
			return True


