from dataclasses import dataclass

@dataclass
class ConnectionHandle:
    host: str
    port: int




def SQLConnect()
    return ConnectionHandle

SQLRETURN SQLConnect(  
     SQLHDBC        ConnectionHandle,  
     SQLCHAR *      ServerName,  
     SQLSMALLINT    NameLength1,  
     SQLCHAR *      UserName,  
     SQLSMALLINT    NameLength2,  
     SQLCHAR *      Authentication,  
     SQLSMALLINT    NameLength3); 