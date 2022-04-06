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

    fetch_interval_list = list(inverse_fetch_interval_dict)
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


def gen_action_time_list(config_params, ohlcv_df_dict):
    temp_df = ohlcv_df_dict['base'][config_params['action_timeframe']][config_params['base']['symbol'][0]]
    action_time_list = temp_df['time'].to_list()[1:]
    
    return action_time_list


def get_timeframe_list(symbol_type, config_params):
    open_timeframe_list = list(config_params[symbol_type]['open'])
    close_timeframe_list = list(config_params[symbol_type]['close'])
    timeframe_list = open_timeframe_list + close_timeframe_list

    for stop_key in ['tp', 'sl']:
        if (symbol_type == 'base') & (config_params[stop_key]['signal'] != None):
            stop_timeframe = config_params[stop_key]['signal']['timeframe']
            timeframe_list += [stop_timeframe]
        
    timeframe_list = list(set(timeframe_list))

    return timeframe_list


def get_data(exchange, start_date, end_date, start_hour, interval_dict, config_params):
    ohlcv_df_dict = {
        'base': {},
        'lead': {}
        }
    
    for symbol_type in ['base', 'lead']:
        symbol_list = config_params[symbol_type]['symbol']
        timeframe_list = get_timeframe_list(symbol_type, config_params)

        timeframe_count = 1
        for timeframe in timeframe_list:
            ohlcv_df_dict[symbol_type][timeframe] = {}
            fetch_timeframe, step = get_fetch_timeframe(timeframe, interval_dict)
            limit = int((24 * 60) / interval_dict[fetch_timeframe])
            
            safety_start_dt = start_date - dt.timedelta(minutes=(interval_dict[timeframe] * step * config_params['safety_ohlcv_range']))
            date_list = pd.date_range(safety_start_dt, end_date, freq='d').to_list()
            
            symbol_count = 1
            for symbol in symbol_list:
                ohlcv_df = pd.DataFrame()
                
                date_count = 1
                for date in date_list:
                    since = get_unix_datetime(date, start_hour)
                    temp_df = get_ohlcv_df(exchange, symbol, fetch_timeframe, since, limit)
                    ohlcv_df = pd.concat([ohlcv_df, temp_df])
                    
                    print(f"{symbol_type}: timeframe {timeframe_count}/{len(timeframe_list)} symbol {symbol_count}/{len(symbol_list)} date {date_count}/{len(date_list)}")
                    date_count += 1
                
                if step > 1:
                    ohlcv_df = group_timeframe(ohlcv_df, step)
                    
                ohlcv_df_dict[symbol_type][timeframe][symbol] = ohlcv_df.reset_index(drop=True)
                symbol_count += 1
                
            timeframe_count += 1

    return ohlcv_df_dict