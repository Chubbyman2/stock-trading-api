from django.apps import AppConfig
import threading
from .background_tasks import *


class DailyRecommendationsUpdate(AppConfig):
    name = 'stock_trading_api'

    def ready(self):
        # Run background_task in a different thread
        stock_names = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "TSLA"]
        threading.Thread(target=schedule_dataset_update, args=(stock_names,)).start()
