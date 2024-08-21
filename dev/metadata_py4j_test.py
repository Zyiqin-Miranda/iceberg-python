from pyiceberg.catalog.glue import GLUE_CATALOG_ENDPOINT, GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.io.pyarrow import (
    schema_to_pyarrow,
)
from pyiceberg.types import NestedField, StringType, DoubleType, DecimalType, IntegerType, BooleanType
from typing import Optional
import boto3

CATALOG_NAME = "glue"
# DATABASE_NAME = "zyiqin"
DATABASE_NAME = None
TABLE_NAME = "metadata-py4j-serialize"
def get_s3_path(bucket_name: str, database_name: Optional[str] = None, table_name: Optional[str] = None) -> str:
    result_path = f"s3://{bucket_name}"
    if database_name is not None:
        result_path += f"/{database_name}.db"

    if table_name is not None:
        result_path += f"/{table_name}"
    return result_path

def get_bucket_name():
    return "metadata-py4j-zyiqin1"
    # return "compaction-artifacts-bdtray-beta-us-east-1-zyiqin"
def get_credential():
    boto3_session = boto3.Session()
    credentials = boto3_session.get_credentials()
    return credentials
def get_glue_catelog():
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    print(f"access_key_id: {access_key_id}")
    glue_catalog = GlueCatalog(
        CATALOG_NAME, **{"warehouse": get_s3_path(get_bucket_name(), DATABASE_NAME, TABLE_NAME),
                         "region_name": "us-east-1",
                         "access_key_id": access_key_id,
                         "secret_access_key": secret_access_key,
                         "session_token": session_token}
    )
    return glue_catalog

def get_table_schema_simple():
    return Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        schema_id=1
    )

# def create_glue_table():
#     identifier = (DATABASE_NAME, TABLE_NAME)
#     test_catalog = get_glue_catelog()
#     # test_catalog.create_namespace(DATABASE_NAME)
#     client = boto3.client("s3")
#     response = client.create_bucket(
#         Bucket=get_bucket_name(),
#     )
#     print(response)
#     table_schema_simple = get_table_schema_simple()
#     test_catalog.create_table(identifier, table_schema_simple,
#                               get_s3_path(get_bucket_name(), DATABASE_NAME, TABLE_NAME))
#     table = test_catalog.load_table(identifier)
#     print(table)


def create_new_metadata():
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.io import load_file_io
    from pyiceberg.serializers import ToOutputFile
    from pyiceberg.catalog import Catalog
    import uuid

    # Ideally, we want to set this to correct metadata version if it already existed
    # TODO: Change Icerberg metadata version to correct version
    previous_metadata_version = 0

    schema: Schema = get_table_schema_simple()
    partition_spec = UNPARTITIONED_PARTITION_SPEC
    METADATA_LOCATION = f"s3://metadata-py4j-zyiqin1"
    UNIQUE_ICEBERG_TABLE_ID = "5a4e4558-b4a0-47aa-849b-00123aab067d"
    table_metadata = new_table_metadata(location=METADATA_LOCATION,
                                        schema=schema,
                                        partition_spec=partition_spec,
                                        sort_order=UNSORTED_SORT_ORDER,
                                        properties={},
                                        table_uuid=UNIQUE_ICEBERG_TABLE_ID)

    # metadata_location = Catalog._get_metadata_location(METADATA_LOCATION, previous_metadata_version)
    location = METADATA_LOCATION
    version_str = f"{previous_metadata_version:05d}"
    metadata_location = f"{location}/metadata/{version_str}-{uuid.uuid4()}.metadata.json"

    print(f"first_metadata_location:{metadata_location}")

    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    io = load_file_io(properties={"s3.access-key-id": access_key_id,
                         "s3.secret-access-key": secret_access_key,
                         "s3.session-token": session_token}, location=metadata_location)
    ToOutputFile.table_metadata(table_metadata, io.new_output(metadata_location))

def load_table_based_on_metadata():
    from pyiceberg.table import StaticTable
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    metadata_location = "s3://metadata-py4j-zyiqin1/metadata/00000-eb99a854-9c95-471b-a5de-8dcc35aa371d.metadata.json"
    static_table = StaticTable.from_metadata(metadata_location, properties={"s3.access-key-id": access_key_id,
                         "s3.secret-access-key": secret_access_key,
                         "s3.session-token": session_token})
    print(static_table.metadata)
    return static_table.metadata

def push_metadata_to_py4j_gateway():
    from py4j.java_gateway import JavaGateway, GatewayParameters
    import json
    table_metadata = load_table_based_on_metadata()
    table_metadata_json = table_metadata.model_dump_json()
    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25335))
    stack = gateway.entry_point.getStack()
    stack.waitUntil(table_metadata_json)


# create_new_metadata()
push_metadata_to_py4j_gateway()