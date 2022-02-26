import numpy as np
import pandas as pd
import datetime as dt
from dateutil import tz


def convert_tz(utc):
    '''
    Transform utc timezone in exchange to local timezone.
    '''
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    base_ts = utc.replace(tzinfo=from_zone).astimezone(to_zone)
    
    return base_ts


def floor_dt(timestamp, round_minute):
    '''
    Round timestamp to previous minute interval.
    '''
    delta = dt.timedelta(minutes=round_minute)
    round_timestamp = timestamp - (timestamp - dt.datetime.min) % delta

    return round_timestamp


def get_unix_datetime(dt_date, start_hour):
    dt_datetime = dt.datetime(dt_date.year, dt_date.month, dt_date.day, start_hour)
    unix_datetime = dt_datetime.timestamp() * 1000
    
    return unix_datetime


def get_fetch_timeframe(action_timeframe, interval_dict):
    fetch_interval_dict = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '1h': 60,
        '4h': 240,
        '1d': 1440
    }

    inverse_fetch_interval_dict = {v: k for k, v in fetch_interval_dict.items()}

    fetch_interval_list = list(inverse_fetch_interval_dict.keys())
    fetch_interval_list.sort(reverse=True)

    for fetch_interval in fetch_interval_list:
        if interval_dict[action_timeframe] % fetch_interval == 0:
            break

    fetch_timeframe = inverse_fetch_interval_dict[fetch_interval]
    step = int(interval_dict[action_timeframe] / fetch_interval)
    
    return fetch_timeframe, step

    
def get_ohlcv_df(exchange, symbol, timeframe, since, limit):
    ohlcv = pd.DataFrame(exchange.fetch_ohlcv(symbol, timeframe, since, limit))

    if len(ohlcv) > 0:
        ohlcv_df = pd.DataFrame(ohlcv)
        ohlcv_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
        ohlcv_df['time'] = pd.to_datetime(ohlcv_df['time'], unit='ms')
        ohlcv_df['time'] = ohlcv_df['time'].apply(lambda x: convert_tz(x))

        # Remove timezone after offset timezone
        ohlcv_df['time'] = ohlcv_df['time'].dt.tz_localize(None)
    else:
        ohlcv_df = pd.DataFrame()
        
    return ohlcv_df


def group_timeframe(ohlcv_df, step):
    ohlcv_dict = {'time':[], 'open':[], 'high':[], 'low':[], 'close':[]}
            
    for i in [x for x in range(0, len(ohlcv_df), step)]:
        temp_df = ohlcv_df.iloc[i:min(i + step, len(ohlcv_df)), :].reset_index(drop=True)
        ohlcv_dict['time'].append(temp_df['time'][0])
        ohlcv_dict['open'].append(temp_df['open'][0])
        ohlcv_dict['high'].append(max(temp_df['high']))
        ohlcv_dict['low'].append(min(temp_df['low']))
        ohlcv_dict['close'].append(temp_df['close'][len(temp_df) - 1])

    grouped_ohlcv_df = pd.DataFrame(ohlcv_dict)
    
    return grouped_ohlcv_df    


def get_stop_side(stop_key, side):
    if ((stop_key == 'tp') & (side == 'buy')) | ((stop_key == 'sl') &(side == 'sell')):
        stop_side = 'upper'
    elif ((stop_key == 'tp') & (side == 'sell')) | ((stop_key == 'sl') & (side == 'buy')):
        stop_side = 'lower'

    return stop_side


def get_stop_price_percent(stop_key, stop_side, open_price, stop_price_list, config_params):
    if config_params[stop_key]['price_percent'] != None:
        if stop_side == 'upper':
            price_percent_stop_price = open_price * (1 + (config_params[stop_key]['price_percent'] / 100))
        elif stop_side == 'lower':
            price_percent_stop_price = open_price * (1 - (config_params[stop_key]['price_percent'] / 100))

        stop_price_list.append(price_percent_stop_price)

    return stop_price_list


def get_stop_price_signal(stop_key, stop_side, symbol, signal_time, stop_price_list, ohlcv_df_dict, config_params):
    if config_params[stop_key]['signal'] != None:
        ohlcv_df = ohlcv_df_dict['base'][config_params[stop_key]['signal']['timeframe']][symbol]
        check_df = ohlcv_df[ohlcv_df['time'] <= signal_time]
        check_series = check_df.loc[len(check_df) - 1, :]

        signal = list(config_params[stop_key]['signal']['signal'].keys())[0]

        if (stop_side == 'upper') & (check_series[signal] > check_series['close']) | (stop_side == 'lower') & (check_series[signal] < check_series['close']):
            stop_price_list.append(check_series[signal])

    return stop_price_list


def get_stop_price(stop_key, side, symbol, signal_time, open_price, ohlcv_df_dict, config_params):
    stop_price_list = []

    stop_side = get_stop_side(stop_key, side)
    stop_price_list = get_stop_price_percent(stop_key, stop_side, open_price, stop_price_list, config_params)
    stop_price_list = get_stop_price_signal(stop_key, stop_side, symbol, signal_time, stop_price_list, ohlcv_df_dict, config_params)

    if (stop_side == 'upper') & (len(stop_price_list) > 0):
        stop_price = min(stop_price_list)
    elif (stop_side == 'lower') & (len(stop_price_list) > 0):
        stop_price = max(stop_price_list)
    elif (stop_side == 'upper') & (len(stop_price_list) == 0):
        stop_price = np.inf
    elif (stop_side == 'lower') & (len(stop_price_list) == 0):
        stop_price = 0

    return stop_price


def update_stop_price(side, symbol, signal_time, config_params, position_dict, ohlcv_df_dict):
    for stop_key in ['tp', 'sl']:
        stop_price = get_stop_price(stop_key, side, symbol, signal_time, position_dict[symbol]['open_price'], ohlcv_df_dict, config_params)
            
        if position_dict[symbol][stop_key] != stop_price:
            position_dict[symbol][stop_key] = stop_price
            print(f"     Update {stop_key}: {stop_price}")

    return position_dict