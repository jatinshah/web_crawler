import json
import logging
import boto3
import time
import os
import requests
from lxml import etree
from io import StringIO
from urllib.parse import urljoin, urlparse


logger = logging.getLogger("crawler_logger")
logger.setLevel(logging.DEBUG)

dynamodb = boto3.resource("dynamodb")
sqs_client = boto3.client("sqs")


def _get_response(status_code, body):
    if not isinstance(body, str):
        body = json.dumps(body)
    return {"statusCode": status_code, "body": body}


def _get_body(event):
    try:
        return json.loads(event.get("body", ""))
    except:
        logger.debug("event body could not be JSON decoded.")
        return {}


def _send_to_connection(endpoint_url, connection_id, data):
    gatewayapi = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)
    return gatewayapi.post_to_connection(ConnectionId=connection_id,
            Data=json.dumps(data).encode('utf-8'))


def connection_manager(event, context):
    """
    Handles connecting and disconnecting for the Websocket.
    """
    connectionID = event["requestContext"].get("connectionId")

    if event["requestContext"]["eventType"] == "CONNECT":
        logger.info("Connect requested: {}".format(connectionID))
        return _get_response(200, "Connect successful.")
    elif event["requestContext"]["eventType"] == "DISCONNECT":
        logger.info("Disconnect requested: {}".format(connectionID))
        #TODO: Delete DynamoDB crawl
        return _get_response(200, "Connect successful.")
    else:
        logger.error("Connection manager received unrecognized eventType.")
        return _get_response(500, "Unrecognized eventType.")


def start_crawl(event, context):
    connection_id = event["requestContext"].get("connectionId")
    endpoint_url = "https://" + event["requestContext"]["domainName"] + "/" + event["requestContext"]["stage"]
    logger.info(event)
    logger.info('start_crawl invoked by: {}'.format(connection_id))

    # TODO: Prevent multiple crawls in a single connection
    body = _get_body(event)

    crawl_task = {
        'base_url': body['url'],
        'url': body['url'],
        'depth': body['depth'],
        'parent_url': '',
        'connection_id': connection_id,
        'endpoint_url': endpoint_url
    }
    logger.info(json.dumps(crawl_task))

    print(sqs_client.send_message(QueueUrl=os.getenv('SQS_URL'),
                                  MessageBody=json.dumps(crawl_task)))
    return _get_response(200, "Crawl started")


def crawl_url(event, context):
    logger.info('crawl_url invoked')
    crawl_data = dynamodb.Table("crawled_urls")
    for record in event['Records']:
        body = _get_body(record)
        logger.info(record['body'])
        url = body['url']
        path = urlparse(body['url']).path or '/'
        output = {
            'path': path,
            'parent_url': body['parent_url'],
            'depth': body['depth']
        }

        # TODO: Check if it exists in DynamoDB for this Connection
        # TODO: Ignore if ConnectionID is different
        # urls = crawl_data.get_item(Key={'url': url})

        res = requests.get(url)
        if res.status_code == 200:

            # Add to Dynamo DB
            crawl_data.put_item(Item={
                'url': url,
                'connectionID': body['connection_id'],
            })

            html = str(res.content)
            parser = etree.HTMLParser()
            doc = etree.parse(StringIO(html), parser)
            output['count_a'] = doc.xpath('count(//a)')
            output['count_img'] = doc.xpath('count(//img)')
            output['title'] = doc.findtext('.//title')
            _send_to_connection(body['endpoint_url'], body['connection_id'], output)

            links = doc.xpath('//a/@href')
            depth = int(body['depth'])
            if depth == 0:
                continue
            depth = str(depth - 1)
            for link in links:
                if link.startswith('http'):
                    continue
                crawl_task = {
                    'base_url': body['base_url'],
                    'url': urljoin(body['base_url'], link),
                    'depth': depth,
                    'parent_url': body['url'],
                    'connection_id': body['connection_id'],
                    'endpoint_url': body['endpoint_url']
                }

                # Initiate crawl lambda
                print(sqs_client.send_message(QueueUrl=os.getenv('SQS_URL'),
                                              MessageBody=json.dumps(crawl_task)))

        else:
            # TODO: Handled failed GET requests
            return


def ping(event, context):
    logger.info('Ping requested.')
    crawl_requests_table = dynamodb.Table("crawl_requests")
    crawl_data = dynamodb.Table("crawl_data")
    timestamp = int(time.mktime(time.gmtime()))
    created_at = time.asctime(time.gmtime())
    crawl_data.put_item(Item={
        'url': 'https://www.google.com',
        'timestamp': timestamp,
        'depth': 0,
        'connectionID': 'XXX',
        'parent_url': 'https://gmail.com',
        'created_at' : created_at
    })
    logger.info('Item added to database')
    response = {
        "statusCode": 200,
        "body": "PONG!"
    }

    return response