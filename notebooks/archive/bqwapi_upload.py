import sys
import logging
import grpc_tools.protoc

from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
from google.cloud import bigquery

# Setting up logger level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Files can only be written in /tmp directory hence we will be genrating schema.proto and compiled proto in /tmp
sys.path.append('/tmp')

# Datatypes in Proto are different from those of Bigquery. This is a mapper that maps BQ datatypes to proto datatypes.
# This will need more datatypes as needed
DATA_TYPE_MAPPING = {
    "STRING": "string",
    "INTEGER": "sint64",
    "FLOAT": "float",
    "BYTES": "bytes",
    "TIMESTAMP": "string"
}


# Function to serialize row's data in proto format
def create_row_data(data, schema):
    import schema_pb2
    row = schema_pb2.Schema()
    for field in schema:
        field_name = field.name
        field_value = data.get(field_name)

        # Only set the field if the value is not None
        if field_value is not None:
            setattr(row, field_name, field_value)
    return row.SerializeToString()


# Function to generate proto file basis the schema that a given table has configured
def generate_proto_file(schema, file_path):
    proto_file = "syntax = \"proto2\";\n\npackage schema;\n\n"
    proto_message = "message Schema {\n"

    for index, field in enumerate(schema):
        field_name = field.name
        field_type = field.field_type

        if field_type not in DATA_TYPE_MAPPING:
            raise ValueError(f"Field type {field_type} not supported by Proto yet")
        proto_field = f"  required {DATA_TYPE_MAPPING[field_type]} {field_name} = {index + 1};\n"
        proto_message += proto_field

    proto_message += "}\n"
    proto_message = proto_file + proto_message
    with open(file_path, "w") as proto_file:
        proto_file.write(proto_message)

    logger.info("Proto file generated successfully")


def stream_data_to_bigquery(project_id, dataset_id, table_id, data, bq_cred=None, write_cred=None):
    # Configuring Bigquery client to fetch table's schema
    bq_client = bigquery.Client(credentials=bq_cred)
    table_ref = bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
    schema = table_ref.schema

    # Generating proto file basis the schema that a given table has configured
    proto_file_path = "/tmp/schema.proto"
    generate_proto_file(schema, proto_file_path)

    # Compiling the proto file to generate the python file
    grpc_tools.protoc.main(['protoc', '-I/tmp', '--python_out=/tmp', proto_file_path])
    logger.info("Compiled proto file successfully")

    # THis import cannot be outside the function as the proto file is generated dynamically
    import schema_pb2

    # Following code is used to write data to Bigquery using storage write API but in a streaming fashion
    client = bigquery_storage_v1.BigQueryWriteClient(credentials=write_cred)
    parent = client.table_path(project_id, dataset_id, table_id)
    write_stream = types.WriteStream()
    write_stream.type_ = types.WriteStream.Type.COMMITTED
    write_stream = client.create_write_stream(
        parent=parent, write_stream=write_stream
    )
    stream_name = write_stream.name

    request_template = types.AppendRowsRequest()
    request_template.write_stream = stream_name

    proto_schema = types.ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    schema_pb2.Schema.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema
    request_template.proto_rows = proto_data

    append_rows_stream = writer.AppendRowsStream(client, request_template)

    # Setting batch size to 500
    batch_size = 500
    for i in range(0, len(data), batch_size):
        batch_data = data[i:i + batch_size]

        proto_rows = types.ProtoRows()
        for row in batch_data:
            proto_rows.serialized_rows.append(create_row_data(row, schema))

        request = types.AppendRowsRequest()
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = proto_rows
        request.proto_rows = proto_data

        response_future = append_rows_stream.send(request)
        logger.info(f"Result {response_future.result()}")

    logger.info(f"Data successfully streamed to {project_id}.{dataset_id}.{table_id} table")

    append_rows_stream.close()