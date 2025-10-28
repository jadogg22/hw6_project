import unittest
import json
import boto3
from moto import mock_aws
from widget_processor import S3Storage
from config import ConsumerConfig

class TestS3Storage(unittest.TestCase):

    def setUp(self):
        self.MOCK_BUCKET_NAME = 'test-widgets-s3-bucket'
        self.MOCK_REGION = 'us-east-1'

        # Define a standard widget schema for testing

        self.standard_widget_data = {"type":"create","requestId":"938b45ec-c22f-41d2-8b23-49d905cb4821","widgetId":"632240d7-6726-4793-b350-6b75fda2adf5","owner":"Sue Smith","label":"LVAGDCHGI","description":"TVGMYIFJHKWKHEXHHNUIBZWLPOYUKTNMUUAUTYANZGT","otherAttributes":[{"name":"color","value":"blue"},{"name":"height","value":"345"},{"name":"height-unit","value":"cm"},{"name":"width","value":"610"},{"name":"width-unit","value":"cm"},{"name":"rating","value":"4.8968225"},{"name":"quantity","value":"650"},{"name":"vendor","value":"UAINUAGBHDEROF"},{"name":"note","value":"OIHIZDAQNUIGDUXMEQOQWWZYDJAXOGZYGRLRCBOFTQNULIUDZSHGTJNCIDRZUAMMVAEHNXGMYWQXLHZOXFFEKJCLOXCMGONOINKPZHMLOAFHICWHRJNQZHUFKIROPFWUBMCHNKKJSGPWSDZKNXAWYWUHGNHEIQJFSPCGLRTGXAWYWBHETQJYGDNAEEXKXPGPOPXNELNSWKSFFRZYIOTYNBVUEHHEZFYXDUZCTYWMHKIQUSPQMBGGHPLJRFYXHQHBHDZYKDAVQSBKRLQMZBZXOSLQJBGYCZ"}]}

    @mock_aws
    def test_store_widget_s3(self):
        """Tests that a widget conforming to the standard schema can be stored in S3."""
        # Initialize S3 client and create bucket within the mock context
        s3_client = boto3.client('s3', region_name=self.MOCK_REGION)
        s3_client.create_bucket(Bucket=self.MOCK_BUCKET_NAME)

        # Create a mock ConsumerConfig for S3Storage
        mock_config = ConsumerConfig(
            storage_type='s3',
            bucket_2_name='',
            bucket_3_name=self.MOCK_BUCKET_NAME,
            dynamodb_table_name='',
            region_name=self.MOCK_REGION,
            polling_delay_ms=100
        )

        # Instantiate the S3Storage class
        storage = S3Storage(mock_config)

        storage.store_widget(self.standard_widget_data)

        # Verify the item was stored in the mock S3 bucket
        response = s3_client.get_object(
            Bucket=self.MOCK_BUCKET_NAME,
            Key=f"widgets/{self.standard_widget_data['owner'].replace(' ', '-').lower()}/{self.standard_widget_data['widgetId']}"
        )
        retrieved_body = response['Body'].read().decode('utf-8')
        retrieved_data = json.loads(retrieved_body)

        # Assert that the stored item matches the original data
        self.assertEqual(retrieved_data['widgetId'], self.standard_widget_data['widgetId'])
        self.assertEqual(retrieved_data['owner'], self.standard_widget_data['owner'])
        self.assertEqual(retrieved_data['label'], self.standard_widget_data['label'])
        self.assertEqual(retrieved_data['description'], self.standard_widget_data['description'])
        self.assertEqual(retrieved_data['otherAttributes'], self.standard_widget_data['otherAttributes'])

if __name__ == '__main__':
    unittest.main()
