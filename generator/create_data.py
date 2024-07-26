import random
import datetime
import pytz
import uuid

def _get_id() -> str:
    return str(uuid.uuid4())

def _get_time_data() -> str:
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone('Asia/Seoul'))
    d_now = kst_now.strftime('%m/%d/%Y')

    return d_now

def _get_trax_type() -> str:
    return random.choice(['CASH', 'CARD', 'BITCOIN'])

def _get_amount() -> int: mnbv
    return random.randint(0,100)

def generate_transaction_data() -> dict:
    return {
        'ID' : _get_id(),
        'TRANSACTION_TYPE' : _get_trax_type(),
        'AMOUNT' : _get_amount(),
        'DATE' : _get_time_data(),
        'CURRENCY' : 'USD'
    }
