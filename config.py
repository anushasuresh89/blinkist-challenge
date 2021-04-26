import boto3
import os


# api_key = 'DZkGyNAOA63Um7k_fmeRSuC-TTs' # for my testing purposes
#apptweak api key settings
api_key=os.environ.get("apptweak_api_key")
header = {'X-Apptweak-Key': api_key}
url_ios = 'https://api.apptweak.com/ios/applications/568839295/ratings-history.json'
url_android = 'https://api.apptweak.com/android/applications/com.blinkslabs.blinkist.android/ratings-history.json'


#boto3 settings
access_key = os.environ.get("access_key")
secret = os.environ.get("secret")
region = 'us-west-2'
bucket = os.environ.get("bucket")
key = os.environ.get("key")
s3_bucket = boto3.resource('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret)
s3_obj = s3_bucket.Object(bucket, key)
