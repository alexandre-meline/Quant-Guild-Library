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

def get_nvda_daily_data():
    app = IBKRApp()
    app.connect("127.0.0.1", 7497, 123)
    api_thread = Thread(target=run_loop, args=(app,))
    api_thread.start()
    time.sleep(1)

    contract = Contract()
    contract.symbol = "NVDA"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    end_time = time.strftime("%Y%m%d %H:%M:%S")
    app.reqHistoricalData(
        reqId=1,
        contract=contract,
        endDateTime=end_time,
        durationStr="1 Y",
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=1,        
        formatDate=1,    
        keepUpToDate=False,  
        chartOptions=[],     
    )
    while not app.finished:
        time.sleep(0.5)

    app.disconnect()
    df = pd.DataFrame(app.data)
    df["return"] = df["close"].pct_change()
    df = df.dropna(subset=["return"])
    df = df[["datetime", "open", "high", "low", "close", "volume", "return"]]
    df.to_csv("nvda_daily_ibkr.csv", index=False)
    return df

if __name__ == "__main__":
    get_nvda_daily_data()
