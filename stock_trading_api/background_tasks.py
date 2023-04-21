import schedule
import threading
import time
from .data_extractor import *


def schedule_dataset_update(stock_names):
    '''
    Uses update_dataset once every day at 5:00 p.m. (after the market closes).
    '''
    schedule.every().day.at("17:00").do(update_dataset, stock_names)
    while True:
        # run_pending() is called after the scheduled time has passed
        schedule.run_pending()
        time.sleep(3600) # Wait one hour before checking again 