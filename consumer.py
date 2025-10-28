import boto3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    print("Consumer started")
    loopCondition = True

    while(loopCondition):
        try:
            # get request from S3
            print("Fetching request from S3...")
            # if reqquest is good, process it
            if True:
                print("Processing request...")
                # process create, delete, update
                print("Request processed successfully.")
            else:
                sleep(5)


        except Exception as e:
            print(f"An error occurred: {e}")



if __name__ == "__main__":
    main()
