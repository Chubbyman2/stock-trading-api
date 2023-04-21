import datetime
import pandas as pd
import pymongo
import requests
import yfinance as yf


def get_stock_names():
    '''
    Get all S&P 500 stock names from Wikipedia
    '''
    stock_names = []
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    response = requests.get(url)
    df = pd.read_html(response.text)[0]
    for stock_name in df['Symbol']:
        stock_names.append(stock_name)
    return stock_names


def calculate_technical_indicators(data):
    '''
    Given a dataframe of stock data, calculate the following technical indicators:
    Simple Moving Average (SMA)
    Exponential Moving Average (EMA)
    Moving Average Convergence Divergence (MACD)
    Relative Strength Index (RSI)
    Commodity Channel Index (CCI)
    Average Directional Index (ADX)
    '''
    # Calculate SMA with a period of 10
    sma = data["Close"].rolling(window=10).mean()

    # Calculate EMA with a period of 10
    ema = data["Close"].ewm(span=10, adjust=False).mean()

    # Calculate MACD with fast period of 12, slow period of 26, and signal period of 9
    ema_12 = data["Close"].ewm(span=12, adjust=False).mean()
    ema_26 = data["Close"].ewm(span=26, adjust=False).mean()
    macd = ema_12 - ema_26

    # Calculate RSI with a period of 14
    delta = data["Close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # Calculate the CCI with a period of 20
    typical_price = (data["High"] + data["Low"] + data["Close"]) / 3
    cci = typical_price - typical_price.rolling(window=20).mean()
    cci /= 0.015 * typical_price.rolling(window=20).std()

    # Calculate the ADX
    tr = data["High"] - data["Low"]
    tr1 = data["High"] - data["Close"].shift(1)
    tr2 = data["Low"] - data["Close"].shift(1)
    tr = tr.combine(tr1, max)
    tr = tr.combine(tr2, max)
    tr = tr.fillna(0)
    tr_sum = tr.rolling(window=14).sum()
    tr_sum = tr_sum.fillna(0)
    dm_plus = data["High"] - data["High"].shift(1)
    dm_minus = data["Low"].shift(1) - data["Low"]
    dm_plus = dm_plus.where((dm_plus > 0) & (dm_plus > dm_minus), 0)
    dm_minus = dm_minus.where((dm_minus > 0) & (dm_minus > dm_plus), 0)
    dm_plus = dm_plus.fillna(0)
    dm_minus = dm_minus.fillna(0)
    dm_plus_ewm = dm_plus.ewm(span=14, adjust=False).mean()
    dm_minus_ewm = dm_minus.ewm(span=14, adjust=False).mean()
    di_plus = 100 * dm_plus_ewm / tr_sum
    di_minus = 100 * dm_minus_ewm / tr_sum
    dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
    adx = dx.ewm(span=14, adjust=False).mean()

    # Append the SMA, EMA, MACD, and RSI to the data
    data["SMA"] = sma
    data["EMA"] = ema
    data["MACD"] = macd
    data["RSI"] = rsi
    data["CCI"] = cci
    data["ADX"] = adx

    return data


def dataset_downloader(stock_name):
    '''
    Downloads stock data. Returns a dataframe of the stock data.
    '''
    # Replace all . with - in stock name (e.g. BRK.B -> BRK-B)
    stock_name = stock_name.replace('.', '-')
    data = yf.download(stock_name, progress=False) # progress=false to avoid printing progress bar

    # Calculate technical indicators
    data = calculate_technical_indicators(data)

    return data


def create_dataset(stock_names):
    '''
    Given an array of stock tickers, store the data of each stock to schema in MongoDB. 
    '''
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb+srv://Chubbyman:Chubbyman2@cluster0.g3hhsiw.mongodb.net/?retryWrites=true&w=majority")

    for stock_name in stock_names:
        # Download the data
        data = dataset_downloader(stock_name)

        # Reset the index to make the date a column
        data = data.reset_index()

        # Delete last 10 rows (for testing the updater)
        # data = data[:-10]
        
        # Convert the data to a dictionary
        data_dict = data.to_dict("records")

        # Insert the data into MongoDB
        client["stock_data"][stock_name].insert_many(data_dict)

        
def update_dataset(stock_names):
    '''
    Given an array of stock tickers, update the data of each stock to schema in MongoDB.
    '''
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb+srv://Chubbyman:Chubbyman2@cluster0.g3hhsiw.mongodb.net/?retryWrites=true&w=majority")

    stock_count = 0
    for stock_name in stock_names:
        stock_count += 1
        # Get the date of the latest row of data
        last_date = client["stock_data"][stock_name].find().sort("Date", -1).limit(1)[0]["Date"]

        # Convert datetime.datetime to string
        last_date = last_date.strftime('%Y-%m-%d')

        # Check if today's date is greater than the last date
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        if today <= last_date:
            print("Data for " + stock_name + " is up to date")
            continue

        # If it's not, download the latest data and append it to the csv
        try:
            print("Updating data for " + stock_name + " [" + str(stock_count) + "/" + str(len(stock_names)) + "]")

            # Start date is last_date + one day, end date is tomorrow
            start_date = datetime.datetime.strptime(last_date, '%Y-%m-%d') + datetime.timedelta(days=1)
            end_date = datetime.datetime.today() + datetime.timedelta(days=1)
            data = yf.download(stock_name, start=start_date, end=end_date)

            # Append 27 rows from the end of the previous data to the start of the new data
            # This is because MACD needs EMA with a slow period of 26
            # This is to ensure that the technical indicators are calculated correctly
            prev_data = client["stock_data"][stock_name].find().sort("Date", -1).limit(27)
            prev_data = pd.DataFrame(list(prev_data))
            prev_data = prev_data[["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "SMA", "EMA", "MACD", "RSI", "CCI", "ADX"]]
            
            # Convert data Date column to YYYY-MM-DD format
            data["Date"] = data.index

            # Reverse the order of prev_data
            prev_data = prev_data.iloc[::-1]
            
            # Concatenate the data
            data = pd.concat([prev_data, data])
            data = calculate_technical_indicators(data) # Calculate technical indicators

            # Remove the first 27 rows of the new data
            data = data.iloc[27:]

            # Append to stock data
            data_dict = data.to_dict("records")
            client["stock_data"][stock_name].insert_many(data_dict)
        except Exception as e:
            print("Error updating data for " + stock_name + " [" + str(stock_count) + "/" + str(len(stock_names)) + "]")
            print(e)


if __name__ == "__main__":
    # stock_names = get_stock_names()
    # print(len(stock_names))
    # print(stock_names)

    stock_names = ["GOOGL", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]

    # This will download the entire history for each stock and save it to the data folder
    create_dataset(stock_names)

    # This is the function you run daily to update the dataset with new data
    # update_dataset(stock_names)