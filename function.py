# python 3.7 runtime
from __future__ import print_function

import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import json
import urllib.parse

rekognition = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb')

# --------------- Functions call Rekognition APIs ------------------
def index_faces(bucket, key, imageId):
    response = rekognition.index_faces(CollectionId="[YOUR_COLLECTION_ID]",
                                        Image={"S3Object": {"Bucket": bucket, "Name": key}},
                                        ExternalImageId=imageId,
                                        MaxFaces=1,
                                        QualityFilter="AUTO",
                                        DetectionAttributes=['ALL'])
    print('------------------')
    for faceRecord in response['FaceRecords']:
         print('Face ID: ' + faceRecord['Face']['FaceId'])
         print('Location: {}'.format(faceRecord['Face']['BoundingBox']))

    print('Faces not indexed:')
    for unindexedFace in response['UnindexedFaces']:
        print('Location: {}'.format(unindexedFace['FaceDetail']['BoundingBox']))
        print('Reasons:')
        for reason in unindexedFace['Reasons']:
            print('   ' + reason)
    return response

def search_faces_by_image(bucket, key):
    threshold = 70
    maxFaces = 7
    response = rekognition.search_faces_by_image(CollectionId="[YOUR_COLLECTION_ID]",
                                                Image={"S3Object": {"Bucket": bucket, "Name": key}},
                                                FaceMatchThreshold=threshold,
                                                MaxFaces=maxFaces)
    return response

# --------------- Functions write DynamoDB ------------------
def insertDB(table, activity_id, student_id, item_id):
    print('DynamoDB insert start')
    table.put_item(
        Item = {
            "activity_id": activity_id,
            "student_id": student_id,
            "item_id": item_id
        }
    )
    print('DynamoDB insert end')

# --------------- Main handler ------------------
def lambda_handler(event, context):
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf8')

    folder_str = key.split("/", 1)
    print(folder_str)

    # Only the SELCET folder file will be add to collection
    if folder_str[0] == '[YOUR_FOLDER]':
        str = folder_str[1].split("_", 1)
        index_faces(bucket, key, str[0])
        return

    try:
        response = search_faces_by_image(bucket, key)
        print('------------------')
        print('Rekognition Result')

        print(response)
        print(key)
        str = key.split("_", 1)
        print('------------------')

        faces = response['FaceMatches']
        table = dynamodb.Table('[YOUR_DYNAMODB_TABLE]')
        for face in faces:
            insertDB(table, int(str[0]), int(face['Face']['ExternalImageId']), str[1])

        return
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
