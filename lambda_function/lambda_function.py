from __future__ import print_function
import base64
import boto3
import json
import uuid
from math import trunc
from datetime import datetime

#Custom Imports
from sns_handler import SNSHandler

def lambda_handler(event, context):
    print("Receiving records: " + str(event))
    #return True
    db_rsrc = boto3.resource("dynamodb")
    for record in event["Records"]:
        payload=json.loads(
                base64.b64decode(record["kinesis"]["data"]).decode()
            )
        # payload = {"order_id": 1, "location": {"coord_x": 2, "coord_y": 3}}

        track_id = str(uuid.uuid1().hex)
        
        track_tbl = db_rsrc.Table("order_track")
        order_details_tbl = db_rsrc.Table("order_details") 
        
        payload.update({"id": track_id, "timestamp_ms":get_timestamp_ms()})

        response = track_tbl.put_item(
                Item=payload
            )
        
                    
        order = order_details_tbl.get_item(
                Key = {"order_id": payload["order_id"]}
            )

        order_details_tbl.update_item(
                Key = {"order_id": payload["order_id"]},
                UpdateExpression="SET last_location.coord_x = :coord_x, "\
                                  "last_location.coord_y = :coord_y",
                ExpressionAttributeValues={
                    ":coord_x": payload["location"]["coord_x"],
                    ":coord_y": payload["location"]["coord_y"]
                }
            )
        
        destiny = order["Item"]["final_location"]
        current_location = payload["location"]
        status = check_distance(current_location, destiny)
        if status:
            sns = SNSHandler()
            sns.publishStatusToSNS(status, str(payload["order_id"]))
    
        print("Decoded payload: " + str(payload))


def check_distance(current_location, destiny):
    if current_location["coord_x"] == destiny["coord_x"] and \
    current_location["coord_y"] and destiny["coord_y"]:
        print("NOTIFICAÇÃO: CHEGOU!")
        return "NOTIFICAÇÃO: CHEGOU!"
    elif abs(current_location["coord_x"] - destiny["coord_x"]) <= 1 and \
    abs(current_location["coord_y"] - destiny["coord_y"]) <= 1:
        print("NOTIFICAÇÃO: ESTAMOS PRÓXIMO!")
        return "NOTIFICAÇÃO: ESTAMOS PRÓXIMO!"
    else:
        print("SEM NOTIFICAÇÃO: AINDA DISTANTE.")
        return None
        
def get_timestamp_ms(date = None):
    date = date or datetime.now()
    return trunc(datetime.timestamp(date) * 1000)