import argparse
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import paramiko
from scp import SCPClient
import logging.config
# import watchtower
import redshift_connector
# import yaml


# Initialize logger
# with open('./lib/logging_config.yml', 'r') as stream:
#    config = yaml.load(stream, Loader=yaml.FullLoader)

# logging.config.dictConfig(config)

# logger = logging.getLogger('crawler')

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Argument parsing setup
parser = argparse.ArgumentParser(description='Manage AWS EC2 instances for web crawling.')
parser.add_argument('--action', choices=['create', 'terminate', 'run'], required=True, help='Action to perform: create or terminate instances')
parser.add_argument('--instance_type', default='t2.nano', help='Type of EC2 instance to manage')
parser.add_argument('--key_name', required=False, help='Key pair name for the EC2 instances')
parser.add_argument('--instance_name', required=False, help='Name for the EC2 instances')
parser.add_argument('--security_group', required=False, help='Security group for the EC2 instances')
parser.add_argument('--ami', required=False, help='AMI ID for the EC2 instances')
parser.add_argument('--count', type=int, default=1, help='Number of instances to manage')
parser.add_argument('--aws_ec2_key_file', type=str, help='Private key file to connect to the instance')
parser.add_argument('--aws_profile_name', type=str, help='Profile to load to connecting to aws environment')
parser.add_argument('--aws_ec2_region_name', type=str, help='Name of the region name where EC2 instances are to be invoked')
parser.add_argument('--crawler_local_directory', type=str, help='Root directory of the crawler program locally')
parser.add_argument('--crawler_remote_directory', type=str, help='Root directory of the crawler program remotely')
parser.add_argument('--db_host', required=True, help='Database host')
parser.add_argument('--db_name', required=True, help='Database name')
parser.add_argument('--db_username', required=True, help='Database username')
parser.add_argument('--db_password', required=True, help='Database password')
parser.add_argument('--table_name', required=True, help='Table name in data from where we need to pick websites')
parser.add_argument('--field_name', required=True, help='Fieldname in the table which contains website information')
parser.add_argument('--urlset_id_field_name', required=True, help='URLSet_id field name')
parser.add_argument('--ec2_instance_id', required=False, help='Run on any existing running ec2 instance')
parser.add_argument('--output_location', required=False, help='Output location for the WARC files, either a local path or an S3 bucket (e.g., file://path/to/dir or s3://bucket-name)')


# Parse arguments
args = parser.parse_args()
print(1)
# Create an EC2 client
session = boto3.Session(profile_name=args.aws_profile_name)
print(1.1)
ec2_client = session.client('ec2', region_name=args.aws_ec2_region_name)
print(2)

def get_urls_from_db(db_connection, urlset_id):
    try:
        with db_connection.cursor() as cursor:
            query = f"SELECT {args.field_name} FROM {args.table_name} WHERE {args.urlset_id_field_name} = %s"
            cursor.execute(query, (urlset_id,))
            urls = [row[0] for row in cursor.fetchall()]
            return urls
    except Exception as e:
        logger.error(f"Error retrieving URLs: {e}")
        return []


def get_distinct_urlset_ids(db_connection):
    try:
        with db_connection.cursor() as cursor:
            query = f"SELECT DISTINCT {args.urlset_id_field_name} FROM {args.table_name} limit 1"
            cursor.execute(query)
            urlset_ids = [row[0] for row in cursor.fetchall()]
            return urlset_ids
    except Exception as e:
        logger.error(f"Error retrieving urlset_ids: {e}")
        return []


def write_urls_to_file(urls, filename="urls.txt"):
    with open(filename, 'w') as file:
        for url in urls:
            file.write(url + '\n')


def get_instance_public_ip(instance_id):
    logger.info(f"Fetching public IP of instance: {instance_id}")
    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    return response['Reservations'][0]['Instances'][0]['PublicIpAddress']


def create_scp_client(ssh_client):
    return SCPClient(ssh_client.get_transport())


def run_crawler_on_instance(instance_id, crawler_local_folder, crawler_remote_folder, urlset_id, output_location):
    key = paramiko.RSAKey.from_private_key_file(args.aws_ec2_key_file)
    ip_address = get_instance_public_ip(instance_id)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip_address, username='ubuntu', pkey=key)

    # Create SCP client for file transfer
    with SCPClient(ssh.get_transport()) as scp:
        scp.put(crawler_local_folder, crawler_remote_folder, recursive=True)
        logger.info(f"Folder uploaded...")


    # Execute the crawler script with the necessary parameters
    command = f'python3 {crawler_remote_folder}/process_urls.py --input_urls {crawler_remote_folder}/urls.txt --output_location {output_location}'
    stdin, stdout, stderr = ssh.exec_command(command)
    print(stdout.read())
    print(stderr.read())


def create_instances(ami, instance_type, key_name, security_group, count, instance_name):
    """
    Create EC2 instances.
    """
    try:
        instances = ec2_client.run_instances(
            ImageId=ami,
            InstanceType=instance_type,
            KeyName=key_name,
            SecurityGroupIds=[security_group],
            TagSpecifications=[
                                {
                                    'ResourceType': 'instance',
                                    'Tags': [
                                        {
                                            'Key': 'Name',
                                            'Value': instance_name,
                                        },
                                    ],
                                },
                            ],
            MinCount=count,
            MaxCount=count
        )
        logger.info("Instances created successfully.")
        return instances
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error creating instances: {e}")
        return None


def terminate_instances(instance_ids):
    """
    Terminate EC2 instances.
    """
    try:
        ec2_client.terminate_instances(InstanceIds=[instance_ids])
        logger.info("Instances terminated successfully.")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error terminating instances: {e}")


def create_db_connection():
    try:
        return redshift_connector.connect(
            host=args.db_host,
            database=args.db_name,
            user=args.db_username,
            password=args.db_password
        )
    except Exception as e:
        logger.error(f"Error connecting to Redshift database: {e}")
        return None


def run():
    db_connection = create_db_connection()
    if db_connection:
        try:
            urlset_ids = get_distinct_urlset_ids(db_connection)
            for urlset_id in urlset_ids:
                urls = get_urls_from_db(db_connection, urlset_id)
                write_urls_to_file(urls)
                run_crawler_on_instance(args.ec2_instance_id, args.crawler_local_directory, args.crawler_remote_directory, urlset_id, args.output_location)
        except Exception as e:
            raise e
            logger.error(f"Error in run operation: {e}")
        finally:
            db_connection.close()
    else:
        logger.error("Failed to establish database connection.")


if args.action == 'run':
    run()

if args.action == 'create':
    instances = create_instances(args.ami, args.instance_type, args.key_name, args.security_group, args.count, args.instance_name)
    logger.info(f"Instance: {instances}")

if args.action == 'terminate':
    terminate_instances(args.ec2_instance_id)    
    
    
# AWS_DEFAULT_REGION='ap-south-1' PYTHONPATH=$PYTHONPATH:/Users/shyamperi/Projects/bq/Forked_Implementations/warcio_with_capture_id/ python3 lib/crawler_manager.py --action run --aws_ec2_key_file ~/Documents/pems/FJ_Development.pem --aws_profile_name bq_ai --aws_ec2_region_name "ap-south-1" --crawler_local_directory ../BQ_Crawler --db_host "bq-redshift-prod-a741eb4ce6f34bca.elb.us-west-2.amazonaws.com" --db_name "dev" --db_username username --db_password password --table_name "workspace.bq_website_validation_batched" --field_name "bq_organization_website" --urlset_id_field_name "batch_id" --ec2_instance_id "i-092dad24d0ae5039d" --crawler_remote_directory "/home/ubuntu/WorkSpace/SP"