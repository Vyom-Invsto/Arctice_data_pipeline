import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import arcticdb as adb
from oracle import OracleLiveData
import datetime

def onerror(error_message):
    print(f"Error: {error_message}")

def onclose(close_message):
    print(f"Connection closed: {close_message}")

def onmessage(message, ac, company_symbol):
    try:
        store_tick_data(message, ac, company_symbol)
    except Exception as e:
        onerror(f"Error processing message for {company_symbol}: {e}")

def store_tick_data(data, company_symbol, ac):
    if 'lp' in data:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        symbol = data.get('tk')
        price = float(data.get('lp'))
        
        library_name = f'tick_data_{company_symbol}'
        if library_name not in ac.list_libraries():
            ac.create_library(library_name)
        library = ac[library_name]

        tick_data = {
            'Timestamp': timestamp,
            'Symbol': symbol,
            'Price': price
        }
        
        data_frame = pd.DataFrame([tick_data])
        library.write(company_symbol, data_frame, metadata={'source': 'Live'})
    else:
        pass


def data_ingestion(company_symbol, ac):
    ora = OracleLiveData(broker_name='finvasia')
    
    try:
        ora.login(
            user_id="FA307801",
            password="Vyom@invsto1",
            factor2="44VR726763CN2JS3SAQ5366QLE62H2VA",
            vc="FA307801_U",
            api_key="3ecf70874d69583b02d599b1471c749b",
            imei="abc1234",
        )
    except Exception as e:
        onerror(f"Login failed for {company_symbol}: {e}")
        return
    
    try:
        ora.get_livedata(
            instruments=[company_symbol],
            exchange="NSE",
            onclose=onclose,
            onerror=lambda e: onerror(f"Failed to get live data for {company_symbol}: {e}"),
            onmessage=lambda m: onmessage(m, ac, company_symbol),
            searchscrip=True
        )
    except Exception as e:
        onerror(f"Failed to get live data for {company_symbol}: {e}")
        return

def main():
    companies = ['AAPL', 'GOOGL', 'MSFT']  
    ac = adb.Arctic("lmdb:///Arctic")
    with ThreadPoolExecutor(max_workers=len(companies)) as executor:
        futures = [executor.submit(data_ingestion, company, ac) for company in companies]
    

if __name__ == "__main__":
    main()

