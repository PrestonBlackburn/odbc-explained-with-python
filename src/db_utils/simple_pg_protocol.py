import socket
from typing import List, Tuple, Generator
from dataclasses import dataclass, field

from logging import getLogger
_logger = getLogger(__name__)

# these are created by default in Postgres
parameters = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'database': 'postgres',
}

@dataclass
class ConnectionHandle:
    sock: socket.socket = field(default_factory= lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM))


def create_startup_message(conn_parameters:dict) -> bytes:
    # Establish communication with Postgres server
    # This assumes that there is no auth (ex: POSTGRES_HOST_AUTH_METHOD=trust)

    # Protocol version 3.0 is represented by 196608 (0x00030000)
    protocol_version = 196608

    # Constructing the startup message in the correct binary format
    message = b''
    for key, value in conn_parameters.items():
        message += key.encode('utf-8') + b'\x00'  # key + null terminator
        message += value.encode('utf-8') + b'\x00'  # value + null terminator
    message += b'\x00'  # End of parameters (final null terminator)

    # The total length of the message, including itself (4 bytes) and protocol version (4 bytes)
    total_length = 4 + 4 + len(message)

    # Pack the total length, protocol version, and the message
    startup_message = total_length.to_bytes(4, 'big') + protocol_version.to_bytes(4, 'big') + message

    return startup_message


def receive_all(sock: socket.socket):
    BUFF_SIZE = 4096 # 4 KiB
    data = b''
    while True:
        part = sock.recv(BUFF_SIZE)
        data += part
        if len(part) < BUFF_SIZE:
            # either 0 or end of data
            break
    return data

def startup(conn_parameters: dict, handle: ConnectionHandle) -> socket.socket:
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = handle.sock
    sock.connect((conn_parameters['host'], conn_parameters['port']))

    conn_parameters.pop('host', None)
    conn_parameters.pop('port', None)
    startup_message = create_startup_message(conn_parameters)

    sock.sendall(startup_message)

    # Receive response (e.g., Authentication request or success/failure)
    response = receive_all(sock)
    _logger.debug(f"Startup Response {response}")

    return handle


def create_query_message(query:str) -> bytes:

    query_encoded = query.encode('utf-8') + b'\x00' # query + null terminator

    message_length = 4 + len(query_encoded)

    #simple_query_message = b'Q' + struct.pack('!I', message_length) + query_encoded
    simple_query_message = b'Q' + message_length.to_bytes(4, 'big') + query_encoded

    return simple_query_message


def execute(handle: ConnectionHandle, query:str) -> str:
    sock = handle.sock
    query = create_query_message(query)
    
    sock.sendall(query)

    return sock

def recv_exact(sock: socket.socket, num_bytes:int) -> bytes:
    data = b''
    while len(data) < num_bytes:
        packet = sock.recv(num_bytes - len(data))
        if not packet:
            raise ConnectionError("Connection Lost")
        data += packet
    return data

def fetch_message(handle: ConnectionHandle) -> Generator[bytes, None, None]:
    # Message format:
    # char tag | int32 len | payload
    sock = handle.sock
    message_length = 1
    while message_length > 0:
        # Get the length of the message (4 bytes)
        char_tag = recv_exact(sock, 1)
        _logger.debug(f"got char_tag from sock: {char_tag.decode('utf-8')}")

        length = recv_exact(sock, 4)
        payload_len = int.from_bytes(length, 'big') - 4 # 4 bytes for length
        _logger.debug(f"got length from sock: {payload_len}")

        part = recv_exact(sock, payload_len)
        _logger.debug(f"got part from sock: {part}")

        message_length = len(char_tag) + len(length) + len(part)

        yield char_tag + length + part



def _parse_row_description(message: bytes) -> list:
    # https://www.postgresql.org/docs/current/protocol-message-formats.html
    # Byte1('T') - Identifies the message as a row description.
    # Int32 - Length of message contents in bytes, including self.
    # Int16 - Specifies the number of fields in a row (can be zero).

    # Then, for each field, there is the following:
    # String - The field name.
    # Int32 - If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
    # Int16 - If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
    # Int32 - The object ID of the field's data type.
    # Int16 -The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    # Int32 - The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    # Int16 - The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
    _logger.debug(f"Row Description Raw: {message}")
    idx = 2
    num_fields = int.from_bytes(message[0:idx], 'big')
    _logger.debug(f"Number of Fields: {num_fields}")
    if num_fields > 100:
        # just to catch some simple parsing errors
        raise ValueError("Number of fields is too high")


    field_names = []
    for field in range(num_fields):
        # Field name (null-terminated string)
        field_name_end = message.find(b'\x00', idx)
        field_name = message[idx:field_name_end].decode('utf-8')
        field_names.append(field_name)
        _logger.debug(f"Field name: {field_name}")

        idx = field_name_end + 1
        # Skip all of the other stuff: 
        # table OID (4 bytes),
        # column attribute number (2 bytes)
        # field data type OID (4 bytes)
        # data type size (2 bytes)
        # type modifier (4 bytes)
        # format code (2 bytes)
        idx += 4 + 2 + 4 + 2 + 4 + 2

    return field_names

def _parse_data_row(message: bytes) -> tuple:
    # https://www.postgresql.org/docs/current/protocol-message-formats.html
    # Byte1('D') - Identifies the message as a data row.
    # Int32 - Length of message contents in bytes, including self..
    # Int16 - The number of column values that follow (possibly zero).
    
    # Then, for each column, there is the following:
    # Int32 - The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
    # Byten - The value of the column, in the format indicated by the associated format code. n is the above length.

    _logger.debug(f"Row Raw: {message}")
    idx = 2
    num_col_values = int.from_bytes(message[0:idx], 'big')
    _logger.debug(f"Number of Column Values: {num_col_values}")

    row = []
    for row_num in range(num_col_values):
        # Field length (4 bytes)
        field_length = int.from_bytes(message[idx:idx+4], 'big')
        idx += 4
        
        _logger.debug(f"Row Length: {field_length}")
        if field_length > 100000:
            # just to catch some simple parsing errors
            raise ValueError("Field length is too high")
        
        if field_length == -1:
            row.append("NULL")
            continue
        
        value = message[idx:idx+field_length]
        _logger.debug(f"Row Value: {value.decode('utf-8')}")
        row.append(value.decode('utf-8'))
        idx += field_length

    return tuple(row)

def parse_message(message: bytes) -> Tuple[str, int, bytes]:

    message_type = message[0:1].decode('utf-8')
    message_length = int.from_bytes(message[1:5], 'big')
    message_body = message[5:]

    _logger.info(f"Message Type:  {message_type}")
    _logger.info(f"Message Length: {message_length}")

    return message_type, message_length, message_body


def get_data(cursor: Generator[bytes, None, None]) -> Tuple[list, List[tuple]]:
    # option to use a "cursor" to get data
    message_length = 1
    columns = []
    rows = []
    while message_length > 0:
        message = next(cursor)
        _logger.debug(f"Message: {message}")

        message_type, message_length, message_body = parse_message(message)
        _logger.info(f"Message Type: {message_type}")

        if message_type == "T":
            columns = _parse_row_description(message_body)

        if message_type == "D":
            row = _parse_data_row(message_body)
            rows.append(row)
        
        if message_type == "C": 
            _logger.info("Command Complete")
            return columns, rows

        if message_type == "Z": 
            _logger.info("End of results - ready for next query")
            return columns, rows
        
    return [], [()]


def get_row(cursor: Generator[bytes, None, None]) -> Tuple[list, tuple]:
    columns = []
    row = ()
    message = next(cursor)
    _logger.debug(f"Message: {message}")
    message_type, message_length, message_body = parse_message(message)

    while message_type == "Z" or message_type == "T":
        if message_type == "Z": 
            _logger.debug("End of last results - ready for next query")
            message = next(cursor)
            _logger.debug(f"Message: {message}")
            message_type, message_length, message_body = parse_message(message)
            _logger.info(f"Message Type: {message_type}")
        
        if message_type == "T":
            # Move to next if it is a start of transaction type
            columns = _parse_row_description(message_body)
            message = next(cursor)
            _logger.debug(f"Message: {message}")
            message_type, message_length, message_body = parse_message(message)

    if message_type == "D":
        row = _parse_data_row(message_body)
        return columns, row
    
    if message_type == "C": 
        _logger.info("Command Complete")
        return None, None

    return None, None


def process_chunk(handle: ConnectionHandle) -> Tuple[list, List[tuple]]:
    # https://www.postgresql.org/docs/current/protocol-message-formats.html
    # Decode the TCP response from the PostgreSQL server
    # A Query Message Returns The Following Segments:
    # 'T': (in transaction) Row Description
    # 'D': Data Row
    # 'C': Command Complete
    # 'Z': Ready for Query
    message_generator = fetch_message(handle)

    columns = []
    rows = []
    message_length = 1
    while message_length > 0:
        message = next(message_generator)
        _logger.debug(f"Message: {message}")

        message_type, message_length, message_body = parse_message(message)
        _logger.info(f"Message Type: {message_type}")

        if message_type == "T":
            columns = _parse_row_description(message_body)

        if message_type == "D":
            row = _parse_data_row(message_body)
            rows.append(row)
        
        if message_type == "C": 
            _logger.debug("Command Complete")
            return columns, rows

        if message_type == "Z": 
            _logger.debug("End of results - ready for next query")
            return columns, rows

    return [], [()]


def disconnect(handle: ConnectionHandle) -> None:
    sock = handle.sock
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
    return None


if __name__ == "__main__":
    handle = ConnectionHandle()
    handle = startup(parameters, handle)
    execute(handle, """select 
        table_name,
        table_schema as schema_name, 
        table_catalog as database_name 
    from information_schema.tables limit 5;""")
    columns, rows = process_chunk(handle)
    print("Columns: ", columns)
    print("Rows: ", rows)
    disconnect(handle)
