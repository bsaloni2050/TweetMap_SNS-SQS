import boto3
import json
import collections
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from alchemyapi import AlchemyAPI

access_token = 'XXXX'
access_token_secret = 'xxxxx'
consumer_key = 'xxxx'
consumer_secret = 'xxx'

awsauth = AWS4Auth('xx', 'x','us-west-2', 'es')
alchemyapi = AlchemyAPI()
sqs = boto3.resource('sqs')
dic = collections.OrderedDict()


try:
	queue = sqs.get_queue_by_name(QueueName='testqq')
	while True:
		for msg in queue.receive_messages(WaitTimeSeconds = 10):
			parsed_input = json.loads(msg.body)
			id = parsed_input['id']
			text = parsed_input['text']
			response = alchemyapi.sentiment("text", text)
			coordinates = parsed_input['coordinates']
			status = response.get('status')
			if status == 'ERROR':
				sentiment = "neutral"
			else:
				sentiment = response["docSentiment"]["type"]
			dic['id'] = id
			dic['text'] = text
			dic['coordina'] = coordinates
			dic['senti'] = sentiment
			jsonArray = json.dumps(dic)
			client = boto3.client('sns')
			res = client.create_topic(Name = 'n_tweet')
			topicArn = res['TopicArn'];
			response = client.subscribe(TopicArn= topicArn,Protocol='HTTP',Endpoint= 'http://x/compute')
			response = client.publish(TopicArn=topicArn,Message=str(jsonArray))
			clientone = boto3.client('sns')
			res = client.create_topic(Name = 'res')
			topicArn = res['TopicArn'];
			response = client.subscribe(TopicArn= topicArn,Protocol='HTTP',Endpoint= 'http://xx/response')
			response = client.publish(TopicArn=topicArn,Message=str(jsonArray))
			
except BaseException as e:
	print(str(e))

except attributeerror as e:
	print(str(e))

except keyerror as e:
	print(str(e))			
		
except exception as e:
	print(str(e))
