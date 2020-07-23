import boto3
import json 

class SNSHandler:
    def __init__(self):
        self.__sns = boto3.client("sns")

    def __publishToSNS(self, subject, msg, arn):
        response = self.__sns.publish(
                TopicArn=arn,
                Message=msg,
                Subject=subject
            )
        print(response)
        
    def publishStatusToSNS(self, subject, msg):
        STATUS_TOPIC_ARN = "arn:aws:sns:us-east-1:155945245308:status-delivery"
        return self.__publishToSNS(subject, msg, STATUS_TOPIC_ARN)
        
        