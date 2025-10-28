import unittest
import json
import boto3
from moto import mock_aws
from widget_processor import DynamoDBStorage
from config import ConsumerConfig

class TestDynamoDBStorage(unittest.TestCase):

    def setUp(self):
        self.MOCK_TABLE_NAME = 'test-widgets-table'
        self.MOCK_REGION = 'us-east-1'

        # Define a standard widget schema for testing
        self.standard_widget_data = {"type":"create","requestId":"938b45ec-c22f-41d2-8b23-49d905cb4821","widgetId":"632240d7-6726-4793-b350-6b75fda2adf5","owner":"Sue Smith","label":"LVAGDCHGI","description":"TVGMYIFJHKWKHEXHHNUIBZWLPOYUKTNMUUAUTYANZGT","otherAttributes":[{"name":"color","value":"blue"},{"name":"height","value":"345"},{"name":"height-unit","value":"cm"},{"name":"width","value":"610"},{"name":"width-unit","value":"cm"},{"name":"rating","value":"4.8968225"},{"name":"quantity","value":"650"},{"name":"vendor","value":"UAINUAGBHDEROF"},{"name":"note","value":"OIHIZDAQNUIGDUXMEQOQWWZYDJAXOGZYGRLRCBOFTQNULIUDZSHGTJNCIDRZUAMMVAEHNXGMYWQXLHZOXFFEKJCLOXCMGONOINKPZHMLOAFHICWHRJNQZHUFKIROPFWUBMCHNKKJSGPWSDZKNXAWYWUHGNHEIQJFSPCGLRTGXAWYWBHETQJYGDNAEEXKXPGPOPXNELNSWKSFFRZYIOTYNBVUEHHEZFYXDUZCTYWMHKIQUSPQMBGGHPLJRFYXHQHBHDZYKDAVQSBKRLQMZBZXOSLQJBGYCZ"}]}

    def test_store_widget_dynamodb(self):
        """Tests that a widget conforming to the standard schema can be stored in DynamoDB."""
        with mock_aws():
            # start client and resource within the mock context
            self.dynamodb_client = boto3.client('dynamodb', region_name=self.MOCK_REGION)
            self.dynamodb_resource = boto3.resource('dynamodb', region_name=self.MOCK_REGION)

            # Create a mock DynamoDB table
            self.dynamodb_client.create_table(
                TableName=self.MOCK_TABLE_NAME,
                KeySchema=[
                    {'AttributeName': 'widgetId', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'widgetId', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
            )
            
            # Create a mock ConsumerConfig for DynamoDBStorage
            self.mock_config = ConsumerConfig(
                storage_type='dynamodb',
                bucket_2_name='',
                bucket_3_name='',
                dynamodb_table_name=self.MOCK_TABLE_NAME,
                region_name=self.MOCK_REGION,
                polling_delay_ms=100
            )

            # Instantiate the DynamoDBStorage class
            self.storage = DynamoDBStorage(self.mock_config)

            self.storage.store_widget(self.standard_widget_data)

            # Verify the item was stored in the mock DynamoDB table
            table = self.dynamodb_resource.Table(self.MOCK_TABLE_NAME)
            response = table.get_item(Key={'widgetId': self.standard_widget_data['widgetId']})

            self.assertIn('Item', response)
            stored_item = response['Item']

            # Assert that the stored item matches the original data
            self.assertEqual(stored_item['widgetId'], self.standard_widget_data['widgetId'])
            self.assertEqual(stored_item['owner'], self.standard_widget_data['owner'])
            self.assertEqual(stored_item['label'], self.standard_widget_data['label'])
            self.assertEqual(stored_item['description'], self.standard_widget_data['description'])
            self.assertEqual(stored_item['otherAttributes'], self.standard_widget_data['otherAttributes'])

if __name__ == '__main__':
    unittest.main()
