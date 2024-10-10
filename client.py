# start websocket client
# use unencrypted connection
# # connect to postgres using posgtresql protocol
# # send a query to postgres using the protocol

# Startup:
# [Length] [Protocol Version] [key1] [value1] [key2] [value2] ... [0]
# # ex:
# # 00 00 00 48           (Length: 72 bytes)
# # 00 03 00 00           (Protocol Version: 196608 for version 3.0)
# # user postgres 00      (Parameter: user = postgres)
# # database mydb 00      (Parameter: database = mydb)
# # application_name myapp 00  (Parameter: application_name = myapp)
# # 00                    (End of parameters)

# Simple Query (not really used in real applications)
# [Message Type] [Length] [Query String] [0]
# 'Q' [Length] [Query String] [0]

# Decoding the response:
# Example:  b'T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x011C\x00\x00\x00\rSELECT 1\x00Z\x00\x00\x00\x05I'

# T (Row Description): Describes the result set's columns.
# Byte 1: 'T' (indicates Row Description).
# Next 4 bytes: Length of the message (0x00000021 = 33 bytes).
# Next 2 bytes: Number of fields (columns). In this case, 0x0001 means 1 column.
# For each field:
# Field name (null-terminated string).
# Table OID (4 bytes, ignored here).
# Column attribute number (2 bytes, ignored here).
# Data type OID (4 bytes).
# Data type size (2 bytes).
# Type modifier (4 bytes).
# Format code (2 bytes).

# 2. Decoding the 'D' (Data Row) Message:
# D (Data Row): Contains the actual data for the result set.
# Byte 1: 'D' (indicates Data Row).
# Next 4 bytes: Length of the message.
# Next 2 bytes: Number of columns (1 in this case).
# For each column:
# Field length (4 bytes).
# Data (if the length is not -1, meaning NULL).

# 3. Decoding the 'C' (Command Complete) Message:
# C (Command Complete): Confirms the command's completion.
# Byte 1: 'C' (indicates Command Complete).
# Next 4 bytes: Length of the message.
# The rest: Command tag (e.g., 'SELECT 1').

# 4. Decoding the 'Z' (Ready for Query) Message:
# Z (Ready for Query): Indicates readiness for the next query.
# Byte 1: 'Z' (indicates Ready for Query).
# Next 4 bytes: Length of the message.
# Next 1 byte: Transaction status indicator ('I' means idle).



import socket
import struct
from decode_tcp_response import decode_postgres_resp 

# Define PostgreSQL server and port
host = 'localhost'  # or the IP address of the PostgreSQL server
port = 5432  # default PostgreSQL port

def recvall(sock):
    BUFF_SIZE = 4096 # 4 KiB
    data = b''
    while True:
        part = sock.recv(BUFF_SIZE)
        data += part
        if len(part) < BUFF_SIZE:
            # either 0 or end of data
            break
    return data


# Create a socket connection
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((host, port))

    # Protocol version 3.0 is represented by 196608 (0x00030000)
    protocol_version = 196608

    # Startup parameters: user, database, and optionally application_name
    parameters = {
        'user': 'postgres',
        'database': 'postgres',
    #    'application_name': 'myapp'
    }

    # Constructing the startup message in the correct binary format
    message = b''
    for key, value in parameters.items():
        message += key.encode('utf-8') + b'\x00'  # key + null terminator
        message += value.encode('utf-8') + b'\x00'  # value + null terminator
    
    message += b'\x00'  # End of parameters (final null terminator)

    # The total length of the message, including itself (4 bytes) and protocol version (4 bytes)
    total_length = 4 + 4 + len(message)

    # Pack the total length, protocol version, and the message
    startup_message = struct.pack('!I', total_length) + struct.pack('!I', protocol_version) + message

    # Send the startup message to the PostgreSQL server
    s.sendall(startup_message)

    # Receive response (e.g., Authentication request or success/failure)
    response = s.recv(4096)
    print("Startup Response:", response)

    #query = 'SELECT 1;'
    query = "select table_name, table_schema, table_catalog from information_schema.tables limit 2;"

    query_encoded = query.encode('utf-8') + b'\x00'

    message_length = 4 + len(query_encoded) # what about the "Q"

    simple_query_message = b'Q' + struct.pack('!I', message_length) + query_encoded

    s.sendall(simple_query_message)

    #response = s.recv(4096)
    response = recvall(s)
    print("Query Response: \n", response)
    decode_postgres_resp(response)




# # odbc standard functions


# # python pep 249 standard apis
