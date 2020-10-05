import boto3
import csv
import hashlib

s3 = boto3.resource('s3',
 aws_access_key_id='aws_access_key_id',
 aws_secret_access_key='aws_secret_access_key'
)

try:

 	s3.create_bucket(Bucket='datacont-cabeeche', CreateBucketConfiguration={
 	'LocationConstraint': 'us-east-1'})
except:
	print ("this may already exist")


bucket = s3.Bucket("datacont-cabeeche")
bucket.Acl().put(ACL='public-read')

## Upload a file into bucket.

s3.Object('datacont-cabeeche', 'jupiter.jpg').put(
	Body=open('/Users/cameronbeeche/Desktop/Fall/cs1674/hw5/jupiter.jpg', 'rb'))


## Create new table

dyndb = boto3.resource('dynamodb',
 			region_name='us-east-1',
 			aws_access_key_id='aws_access_key_id',
 			aws_secret_access_key='aws_secret_access_key'
)

# Create the DynamoDB table.
try:
	print('create table')
	table = dyndb.create_table(
	    TableName='DataTable',
	    KeySchema=[
	        {
	            'AttributeName': 'PartitionKey',
	            'KeyType': 'HASH'
	        },
	        {
	            'AttributeName': 'RowKey',
	            'KeyType': 'RANGE'
	        }
	    ],
	    AttributeDefinitions=[
	        {
	            'AttributeName': 'PartitionKey',
	            'AttributeType': 'S'
	        },
	        {
	            'AttributeName': 'RowKey',
	            'AttributeType': 'S'
	        },
	    ],
	    ProvisionedThroughput={
	        'ReadCapacityUnits': 5,
	        'WriteCapacityUnits': 5
	    }
	)
except:
 	#if there is an exception, the table may already exist. if so...
 	table = dyndb.Table("DataTable")


# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='Datatable')


print(table.item_count)


## Read in csv file

with open('/Users/cameronbeeche/Desktop/Fall/cs1660/cloud/experiments.csv', 'rt') as csvfile:

	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')

	for item in csvf:
		print(item)
		body = open('/Users/cameronbeeche/Desktop/Fall/cs1660/cloud/datafiles/'+item[3], 'rb')

		s3.Object('datacont-cabeeche', item[3]).put(Body=body)

		md = s3.Object('datacont-cabeeche', item[3]).Acl().put(ACL='public-read')

		url = " https://s3-us-east-1.amazonaws.com/datacont-cabeeche/"+item[3]
		
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[2], 'url':url}

		try:
			table.put_item(Item=metadata_item)
		except:
			print("Item may already be there")	
		 		


response = table.get_item( Key = {'PartitionKey': 'experiment2', 'RowKey': '4'})

print(response['Item'])





print('End of file')