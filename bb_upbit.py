import pandas as pd
import numpy as np
import requests
import pyupbit
import time
import json
from datetime import datetime, timedelta

def fetch_upbit_candles(market, count, to=None):
    """
    Upbit 5분봉 데이터를 가져오는 함수
    """
    url = "https://api.upbit.com/v1/candles/minutes/5"
    params = {
        "market": market,
        "count": count,  # 요청할 데이터 개수 (최대 200)
    }
    if to:
        params["to"] = to  # 특정 시간 이전 데이터 가져오기

    response = requests.get(url, params=params)
    if response.status_code == 200:
        # Upbit에서 받은 JSON 데이터를 DataFrame으로 변환
        data = response.json()
        df = pd.DataFrame(data)
        return df
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

def fetch_multiple_candles_df(market, total_count, to_time = None):
    """
    총 total_count개의 5분봉 데이터를 DataFrame으로 수집
    """
    all_candles_df = pd.DataFrame()  # 결과를 저장할 빈 DataFrame
    # to_time = None

    while len(all_candles_df) < total_count:
        count = min(200, total_count - len(all_candles_df))  # 한 번에 최대 200개씩 가져옴
        new_data = fetch_upbit_candles(market, count, to=to_time)

        if new_data.empty:
            break  # 더 이상 데이터가 없으면 종료

        all_candles_df = pd.concat([all_candles_df, new_data], ignore_index=True)
        to_time = new_data.iloc[-1]["candle_date_time_utc"]  # 마지막 데이터의 UTC 시간으로 업데이트

        if len(all_candles_df) % 1000 == 0:
            print(f"Fetched {len(all_candles_df)} / {total_count} candles...")
        time.sleep(0.1)

    return all_candles_df

def calculate_indicators(df, bb_window, bb_stddev, macd_low, macd_high, macd_sig, rsi):
    # Bollinger Bands
    df['SMA'] = df['close'].rolling(window=bb_window).mean()
    df['stddev'] = df['close'].rolling(window=bb_window).std()
    df['Upper_BB'] = df['SMA'] + (df['stddev'] * bb_stddev)
    df['Lower_BB'] = df['SMA'] - (df['stddev'] * bb_stddev)

    # MACD
    exp1 = df['close'].ewm(span=macd_low, adjust=False).mean()
    exp2 = df['close'].ewm(span=macd_high, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(span=macd_sig, adjust=False).mean()

    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=rsi).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=rsi).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    return df

def send_telegram_message(token, chat_id, message):
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}
    response = requests.post(url, data=payload)
    return response.json()

def main():
    #yfinance symbols
    # symbol = "KRW-BTC"
    # start_date = "2020-01-01"
    # end_date = "2024-11-01"

    with open('keys.json', 'r') as file:
        config = json.load(file)
    access_key = config['upbit_access_key']
    secret_key = config['upbit_secret_key']
    telegram_token = config['telegram_token']
    chat_id = config['chat_id']
    upbit = pyupbit.Upbit(access_key, secret_key)

    #upbit symbols
    symbol = "KRW-BTC"
    #################
    # for day candles
    # start_date = datetime(2020, 1, 1)
    # end_date = datetime(2024,11,1)
    # df = get_historical_data(symbol, start_date, end_date)
    ##################


    df = fetch_multiple_candles_df(symbol, 200)
    df = df[["candle_date_time_kst", "opening_price", "high_price", "low_price", "trade_price", "candle_acc_trade_volume"]]
    df.columns = ["time", "open", "high", "low", "close", "volume"]
    df = df.sort_values("time").reset_index(drop=True)
    ###########################

    bb_window = 10
    bb_stddev = 2
    macd_low = 4
    macd_high = 17
    macd_sig = 9
    rsi = 14

    df = calculate_indicators(df, bb_window, bb_stddev, macd_low, macd_high, macd_sig, rsi)
    

    while True:
        balances = upbit.get_balance(symbol)
        avg_buy_price = upbit.get_avg_buy_price(symbol)
        # print(balances, avg_buy_price)
        if (df['close'].iloc[0] < df['open'].iloc[0] and 
                df['close'].iloc[-1] < df['open'].iloc[-1]): #연속으로 음봉일때
            continue
        if balances == 0:  # 보유량이 없을 때
            if (df['close'].iloc[0] <= df['Lower_BB'].iloc[0] or
                (df['MACD'].iloc[0] > df['Signal_Line'].iloc[0] and
                df['MACD'].iloc[-1] <= df['Signal_Line'].iloc[-1]) or
                df['RSI'].iloc[0] <= 30):
                # 매수 신호
                message = f'구매신호'
                send_telegram_message(telegram_token, chat_id, message)                         
                print('MSG Sent.')  # 메시지 전송 여부 확인
        elif balances > 0:  # 보유량이 있을 떄
            stop_loss_price = avg_buy_price * 0.95
            #if (df['Close'].iloc[i] < df['Open'].iloc[i] and 
            #    df['Close'].iloc[i - 1] < df['Open'].iloc[i - 1]): #연속으로 양봉일때
            #    continue
            if (df['close'].iloc[0] >= df['Upper_BB'].iloc[0] or
                (df['MACD'].iloc[0] < df['Signal_Line'].iloc[0] and
                df['MACD'].iloc[-1] >= df['Signal_Line'].iloc[-1]) or
                df['RSI'].iloc[0] >= 70):
                # 매도 신호(손절 또는 수익)
                message = f'수익 신호'
                send_telegram_message(telegram_token, chat_id, message)                         
                print('MSG Sent.')  # 메시지 전송 여부 확인
            if ((df['close'].iloc[0] <= df['Lower_BB'].iloc[0] and
                df['close'].iloc[-1] <= df['Lower_BB'].iloc[-1]) or
                df['close'].iloc[0] <= stop_loss_price):
                message = f'손절 신호'
                send_telegram_message(telegram_token, chat_id, message)                         
                print('MSG Sent.')  # 메시지 전송 여부 확인
        time.sleep(5)



    # with open ('backtest.txt', 'a') as file:
    #     file.write(f"Symbol: {symbol}")
    #     file.write(f"\nConfiguration: bb_window: {bb_window}, bb_stddev: {bb_stddev}, macd_low: {macd_low}, macd_high: {macd_high}, macd_sig: {macd_sig}")
    #     file.write(f"\nInitial Balance: ${initial_balance:.2f}")
    #     file.write(f"\nFinal Balance: ${final_balance:.2f}")
    #     file.write(f"\nTotal Profit: ${total_profit:.2f} ({total_profit_percentage:.2f}%)")
    #     # file.write(f"\nMaximum Profit: {profit:.2f}%")
    #     file.write(f"\nMaximum Drawdown: {mdd:.2f}%")
    #     file.write(f"\nNumber of trades: {len(trades)}\n\n")

if __name__ == "__main__":
    main()