import time
import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from threading import Thread

class IBKRApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []
        self.req_id = 1
        self.finished = False

    def historicalData(self, reqId, bar):
        self.data.append({
            "datetime": bar.date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })

    def historicalDataEnd(self, reqId, start, end):
        self.finished = True

def run_loop(app):
    app.run()

def get_spy_hourly_data():
    app = IBKRApp()
    app.connect("127.0.0.1", 7497, 123)
    api_thread = Thread(target=run_loop, args=(app,))
    api_thread.start()
    time.sleep(1)  # let connection establish

    contract = Contract()
    contract.symbol = "SPY"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    
    # IBKR only allows up to 1 year of hourly data in one request, so we need to loop
    end_time = pd.Timestamp.now(tz='US/Eastern')
    periods = 3
    all_data = []

    for i in range(periods):
        end_time_str = end_time.strftime("%Y%m%d %H:%M:%S")
        app.finished = False
        app.data = []
        app.reqHistoricalData(
            reqId=app.req_id, 
            contract=contract, 
            endDateTime=end_time_str,
            durationStr="1 Y", 
            barSizeSetting="1 hour",
            whatToShow="TRADES", 
            useRTH=0, 
            formatDate=1,
            keepUpToDate=False, 
            chartOptions=[]
        )
        while not app.finished:
            time.sleep(0.5)
        all_data = app.data + all_data  # prepend, so oldest first
        end_time = pd.to_datetime(app.data[0]['datetime'])
        app.req_id += 1
        time.sleep(2)  # avoid pacing violation

    app.disconnect()
    df = pd.DataFrame(all_data)
    df = df.drop_duplicates(subset=["datetime"])
    df = df.sort_values(by="datetime")
    df.to_csv("spy_hourly_ibkr.csv", index=False)

if __name__ == "__main__":
    get_spy_hourly_data()