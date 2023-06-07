import tablestore

ots_client: tablestore.OTSClient

def init_tb_client(end_point, access_key_id, access_key_secret, instance_name):
    global ots_client
    ots_client = tablestore.OTSClient(end_point, access_key_id, access_key_secret, instance_name, logger_name = 'table_store.log',  retry_policy = tablestore.DefaultRetryPolicy())

def client(func):
    def wrapper(*args, **kwargs):
        global ots_client
        if ots_client:
            kwargs['_tb_client'] = ots_client
        return func(*args, **kwargs)
    return wrapper