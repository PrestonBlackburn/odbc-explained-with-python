# https://www.postgresql.org/docs/current/protocol-flow.html

import struct


def _handle_row_description(response, idx):
    # full description of based on the postgres protocol
    print("Row Description:")
    # Number of fields (2 bytes)
    num_fields = struct.unpack('!H', response[idx:idx+2])[0]
    idx += 2
    print(f"Number of fields: {num_fields}")
    
    for _ in range(num_fields):
        # Field name (null-terminated string)
        field_name_end = response.find(b'\x00', idx)
        field_name = response[idx:field_name_end].decode('utf-8')
        idx = field_name_end + 1
        print(f"Field name: {field_name}")

        # Skip table OID (4 bytes), column attribute number (2 bytes)
        idx += 6

        # Data type OID (4 bytes)
        data_type_oid = struct.unpack('!I', response[idx:idx+4])[0]
        idx += 4
        print(f"Data type OID: {data_type_oid}")

        # Data type size (2 bytes)
        data_type_size = struct.unpack('!H', response[idx:idx+2])[0]
        idx += 2
        print(f"Data type size: {data_type_size}")

        # Type modifier (4 bytes) and format code (2 bytes)
        idx += 6
    return idx

def _handle_data_row(response, idx):
    print("Data Row:")
    # Number of columns (2 bytes)
    num_columns = struct.unpack('!H', response[idx:idx+2])[0]
    idx += 2
    print(f"Number of columns: {num_columns}")

    for _ in range(num_columns):
        # Column length (4 bytes)
        col_length = struct.unpack('!I', response[idx:idx+4])[0]
        idx += 4

        if col_length == -1:
            print("Column data: NULL")
        else:
            # Column data (col_length bytes)
            col_data = response[idx:idx+col_length]
            try:
                # Try to decode as UTF-8
                print(f"Column data (text): {col_data.decode('utf-8')}")
            except UnicodeDecodeError:
                # Handle binary or non-UTF-8 data
                print(f"Column data (binary): {col_data}")
            idx += col_length
    return idx

def _handle_command_complete(response, idx):
    print("Command Complete:")
    # Command tag (null-terminated string)
    command_end = response.find(b'\x00', idx)
    command_tag = response[idx:command_end].decode('utf-8')
    idx = command_end + 1
    print(f"Command: {command_tag}")
    return idx

def _handle_ready_for_query(response, idx):
    print("Ready for Query:")
    # Transaction status (1 byte)
    transaction_status = response[idx:idx+1].decode('utf-8')
    idx += 1
    print(f"Transaction status: {transaction_status}")
    return idx


def decode_postgres_resp(response):
    # Decode the TCP response from the PostgreSQL server
    # Message Types
    # 'T': Row Description
    # 'D': Data Row
    # 'C': Command Complete
    # 'Z': Ready for Query

    idx = 0
    while idx < len(response):
        message_type = response[idx:idx+1].decode('utf-8')
        idx += 1
        # Read the length of the message (4 bytes)
        message_length = struct.unpack('!I', response[idx:idx+4])[0]
        idx += 4

        if message_type == 'T':
            idx = _handle_row_description(response, idx)
        elif message_type == 'D':
            idx = _handle_data_row(response, idx)
        elif message_type == 'C':
            idx = _handle_command_complete(response, idx)
        elif message_type == 'Z':
            idx = _handle_ready_for_query(response, idx)
        
    return None




# # Example binary data (your complex response)
# response_data = b'T\x00\x00\x01\xb5\x00\x0ctable_catalog\x00\x00\x004|\x00\x01\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00table_schema\x00\x00\x004|\x00\x02\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00table_name\x00\x00\x004|\x00\x03\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00table_type\x00\x00\x004|\x00\x04\x00\x00\x04\x13\xff\xff\xff\xff\xff\xff\x00\x00self_referencing_column_name\x00\x00\x004|\x00\x05\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00reference_generation\x00\x00\x004|\x00\x06\x00\x00\x04\x13\xff\xff\xff\xff\xff\xff\x00\x00user_defined_type_catalog\x00\x00\x004|\x00\x07\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00user_defined_type_schema\x00\x00\x004|\x00\x08\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00user_defined_type_name\x00\x00\x004|\x00\t\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00is_insertable_into\x00\x00\x004|\x00\n\x00\x00\x04\x13\xff\xff\x00\x00\x00\x07\x00\x00is_typed\x00\x00\x004|\x00\x0b\x00\x00\x04\x13\xff\xff\x00\x00\x00\x07\x00\x00commit_action\x00\x00\x004|\x00\x0c\x00\x00\x04\x13\xff\xff\xff\xff\xff\xff\x00\x00D\x00\x00\x00c\x00\x0c\x00\x00\x00\x08postgres\x00\x00\x00\npg_catalog\x00\x00\x00\x0cpg_statistic\x00\x00\x00\nBASE TABLE\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x03YES\x00\x00\x00\x02NO\xff\xff\xff\xffD\x00\x00\x00^\x00\x0c\x00\x00\x00\x08postgres\x00\x00\x00\npg_catalog\x00\x00\x00\x07pg_type\x00\x00\x00\nBASE TABLE\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x03YES\x00\x00\x00\x02NO\xff\xff\xff\xffC\x00\x00\x00\rSELECT 2\x00Z\x00\x00\x00\x05I'

# # Decode the response
# decode_postgresql_response(response_data)

# # Example binary data
# #response_data = b'T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x011C\x00\x00\x00\rSELECT 1\x00Z\x00\x00\x00\x05I'
response_data =  b'T\x00\x00\x00b\x00\x03table_name\x00\x00\x004|\x00\x03\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00table_schema\x00\x00\x004|\x00\x02\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00table_catalog\x00\x00\x004|\x00\x01\x00\x00\x00\x13\x00@\xff\xff\xff\xff\x00\x00D\x00\x00\x000\x00\x03\x00\x00\x00\x0cpg_statistic\x00\x00\x00\npg_catalog\x00\x00\x00\x08postgresD\x00\x00\x00+\x00\x03\x00\x00\x00\x07pg_type\x00\x00\x00\npg_catalog\x00\x00\x00\x08postgresC\x00\x00\x00\rSELECT 2\x00Z\x00\x00\x00\x05I'


# Decode the response
decode_postgres_resp(response_data)
