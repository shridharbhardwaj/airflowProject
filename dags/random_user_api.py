import requests
import pandas as pd
import boto3
from io import StringIO

###################### Get S3 client #####################
s3= boto3.resource(service_name='s3',region_name='us-east-1',
aws_access_key_id = 'FAKEKEYIFZLXQCWr4',
aws_secret_access_key = 'LQuFAKEKEYEXPOSEDb09ZmZUJanY3457HwxWNgaQxX6Yr4e')

# *******************Writing the sript to fetch data from api************************

def run_userdata_etl():
    url="https://randomuser.me/api"
    results_to_fetch=100


    user_list = []

    # Fetch data until we have 200 users details
    while len(user_list) < results_to_fetch:
        response = requests.get(url)
        data = response.json()['results'][0]  # Access the first user details


        # Check if the data contains the expected fields
        if 'name' in data and 'location' in data and 'gender' in data:
            refined_details = {
                "user_prefix": data['name']['title'],
                "first_name": data['name']['first'],
                "last_name": data['name']['last'],
                "gender": data['gender'],
                "street_number": data['location']['street']['number'],
                "street_name": data['location']['street']['name'],
                "city": data['location']['city'],
                "state": data['location']['state'],
                "country": data['location']['country'],
                "postcode": data['location']['postcode'],
                "email": data['email'],
                "uuid": data['login']['uuid'],
                "phone": data['phone'],
                "cell": data['cell'],
                "nationality": data['nat']

                }
        
            user_list.append(refined_details)
    
    # Create a DataFrame and save to CSV
    df=pd.DataFrame(user_list)
    df.to_csv("random_user_data_files.csv")

    s3.Bucket('project-d-testing').put_object(Body=StringIO(df.to_csv(index=False)).getvalue(), Key='random_user_data_files.csv')
