import unittest
import json
import boto3
from moto import mock_aws
from get_widget import S3RequestRetriever
from config import ConsumerConfig

# all tests came from a cool library called moto that can mock aws services for testing
# i found a cool articl walking though it here https://caylent.com/blog/mocking-aws-calls-using-moto

class TestWidgetSchema(unittest.TestCase):

    def setUp(self):
        """Sets up mock AWS environment, credentials, and S3 bucket."""
        self.MOCK_BUCKET_NAME = 'test-widget-requests'
        self.MOCK_REGION = 'us-east-1'
        
        # Define a standard widget schema for testing
        self.standard_widget_request = {"type":"create","requestId":"938b45ec-c22f-41d2-8b23-49d905cb4821","widgetId":"632240d7-6726-4793-b350-6b75fda2adf5","owner":"Sue Smith","label":"LVAGDCHGI","description":"TVGMYIFJHKWKHEXHHNUIBZWLPOYUKTNMUUAUTYANZGT","otherAttributes":[{"name":"color","value":"blue"},{"name":"height","value":"345"},{"name":"height-unit","value":"cm"},{"name":"width","value":"610"},{"name":"width-unit","value":"cm"},{"name":"rating","value":"4.8968225"},{"name":"quantity","value":"650"},{"name":"vendor","value":"UAINUAGBHDEROF"},{"name":"note","value":"OIHIZDAQNUIGDUXMEQOQWWZYDJAXOGZYGRLRCBOFTQNULIUDZSHGTJNCIDRZUAMMVAEHNXGMYWQXLHZOXFFEKJCLOXCMGONOINKPZHMLOAFHICWHRJNQZHUFKIROPFWUBMCHNKKJSGPWSDZKNXAWYWUHGNHEIQJFSPCGLRTGXAWYWBHETQJYGDNAEEXKXPGPOPXNELNSWKSFFRZYIOTYNBVUEHHEZFYXDUZCTYWMHKIQUSPQMBGGHPLJRFYXHQHBHDZYKDAVQSBKRLQMZBZXOSLQJBGYCZ"}]}

    def test_retrieve_standard_widget_schema(self):
        """Tests that a widget conforming to the standard schema can be retrieved and parsed."""
        with mock_aws():
            # start client and retriever within the mock context
            self.s3_client = boto3.client('s3', region_name=self.MOCK_REGION)
            self.s3_client.create_bucket(Bucket=self.MOCK_BUCKET_NAME)
            self.retriever = S3RequestRetriever(
                bucket_name=self.MOCK_BUCKET_NAME, 
                region_name=self.MOCK_REGION
            )

            self.s3_client.put_object(
                Bucket=self.MOCK_BUCKET_NAME,
                Key=self.standard_widget_request["requestId"],
                Body=json.dumps(self.standard_widget_request)
            )

            retrieved_request = self.retriever.get_and_delete_next_request()

            self.assertIsNotNone(retrieved_request)
            self.assertEqual(retrieved_request, self.standard_widget_request)

            # Verify it was deleted
            remaining_objects = self.s3_client.list_objects_v2(Bucket=self.MOCK_BUCKET_NAME)
            self.assertNotIn('Contents', remaining_objects)

if __name__ == '__main__':
    unittest.main()
