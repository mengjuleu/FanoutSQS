import json

from boto3.session import Session


class FanoutSQS(object):
    def __init__(self,
                 aws_access_key_id,
                 aws_secret_access_key,
                 aws_region_name,
                 topic):
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_region_name = aws_region_name
        self._account_id = self._get_account_id()
        self._topic = topic
        self._sns_topic_arn = 'arn:aws:sns:{}:{}:{}'.format(
            self._aws_region_name,
            self._account_id,
            self._topic)

        self._subscribers = {}

    def send_message(self, message, attributes=None):
        session = self._create_session()
        sns_client = session.client('sns')
        sns_client.publish(
            TopicArn=self._sns_topic_arn,
            Message=message,
            MessageAttributes=attributes
        )

    def subscribe(self, sqs_queue_name):
        session = self._create_session()
        sqs_resource = session.resource('sqs')
        sns_client = session.client('sns')

        # Get SQS Arn
        queue = sqs_resource.get_queue_by_name(QueueName=sqs_queue_name)
        queue_arn = queue.attributes['QueueArn']
        
        # Allow SNS sends message to SQS
        self._set_sqs_policy(
            sqs_queue=queue,
            sqs_queue_arn=queue_arn,
        )

        # Subscribe SQS to SNS
        sns_client.subscribe(
            TopicArn=self._sns_topic_arn,
            Protocol='sqs',
            Endpoint=queue_arn
        )

        self._subscribers[sqs_queue_name] = queue

    def unsubscribe(self, sqs_queue_name):
        session = self._create_session()
        sns_client = session.client('sns')
        resp = sns_client.unsubscribe(
            SubscriptionArn=self._get_subscriber_arn(sqs_queue_name)
        )

        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception('Failed to unsubscribe {}'.format(sqs_queue_name))

    def list_subscription(self):
        session = self._create_session()
        sns_client = session.client('sns')
        resp = sns_client.list_subscriptions_by_topic(
            TopicArn=self._sns_topic_arn
        )
        return resp.get('Subscriptions')

    def _set_sqs_policy(self, sqs_queue, sqs_queue_arn):
        """Allow SNS sends message to sqs
        """
        sqs_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "sqs:SendMessage",
                "Resource": sqs_queue_arn,
                "Condition": {
                    "ArnEquals": {
                        "aws:SourceArn": self._sns_topic_arn
                    }
                }
            }]
        }

        sqs_queue.set_attributes(
            Attributes={
                'Policy': json.dumps(sqs_policy)
            }
        )

    def _get_account_id(self):
        session = self._create_session()
        sts_client = session.client('sts')
        return sts_client.get_caller_identity().get('Account')

    def _create_session(self):
        session = Session(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            region_name=self._aws_region_name)
        return session

    def _get_subscriber_arn(self, subscriber):
        if subscriber not in self._subscribers:
            raise Exception('Subscriber not found')

        queue = self._subscribers[subscriber]
        subscriber_arn = queue.attributes['QueueArn']
        return subscriber_arn


