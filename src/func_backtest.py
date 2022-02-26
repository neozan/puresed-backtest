import pandas as pd
import datetime as dt

from func_get import get_unix_datetime, get_fetch_timeframe, get_ohlcv_df, group_timeframe, get_stop_price, update_stop_price
from func_signal import add_sma, add_ema, add_tma, add_supertrend, add_wt, add_rsi


def gen_action_time_list(config_params, ohlcv_df_dict):
    temp_df = ohlcv_df_dict['base'][config_params['action_timeframe']][config_params['base']['symbol'][0]]
    action_time_list = temp_df['time'].to_list()[1:]
    
    return action_time_list


def get_max_open_timeframe(config_params, interval_dict):
    timeframe_list = list(config_params['base']['open'].keys())
    interval_list = [interval_dict[timeframe] for timeframe in timeframe_list]
    max_interval = max(interval_list)
    
    inverse_dict = {v: k for k, v in interval_dict.items()}
    max_timeframe = inverse_dict[max_interval]
    
    return max_timeframe


def get_timeframe_list(symbol_type, config_params):
    open_timeframe_list = list(config_params[symbol_type]['open'].keys())
    close_timeframe_list = list(config_params[symbol_type]['close'].keys())
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


def add_action_signal(ohlcv_df_dict, func_add_dict, config_params):
    for symbol_type in ['base', 'lead']:
        for timeframe in ohlcv_df_dict[symbol_type].keys():
            for symbol in ohlcv_df_dict[symbol_type][timeframe].keys():
                ohlcv_df = ohlcv_df_dict[symbol_type][timeframe][symbol]
                
                for objective in ['open', 'close']:
                    if timeframe in config_params[symbol_type][objective].keys():
                        for signal in config_params[symbol_type][objective][timeframe].keys():
                            if signal not in ohlcv_df.columns:
                                print(f"{symbol_type} add {signal} to {symbol} {timeframe}")
                                ohlcv_df = func_add_dict[signal](objective, ohlcv_df, timeframe, config_params)

                ohlcv_df_dict[symbol_type][timeframe][symbol] = ohlcv_df

    return ohlcv_df_dict


def add_stop_signal(ohlcv_df_dict, func_add_dict, config_params):
    for objective in ['tp', 'sl']:
        if (config_params[objective]['signal'] != None):
            for symbol in ohlcv_df_dict['base'][config_params[objective]['signal']['timeframe']].keys():
                ohlcv_df = ohlcv_df_dict['base'][config_params[objective]['signal']['timeframe']][symbol]
                signal = list(config_params[objective]['signal']['signal'].keys())[0]
                timeframe = config_params[objective]['signal']['timeframe']

                if signal not in ohlcv_df.columns:
                    print(f"{objective} add {signal} to {symbol} {timeframe}")
                    ohlcv_df = func_add_dict[signal](objective, ohlcv_df, timeframe, config_params)
                    ohlcv_df_dict['base'][timeframe][symbol] = ohlcv_df

    return ohlcv_df_dict


def filter_start_time(start_date, ohlcv_df_dict, interval_dict):
    for symbol_type in ['base', 'lead']:
        for timeframe in ohlcv_df_dict[symbol_type].keys():
            for symbol in ohlcv_df_dict[symbol_type][timeframe].keys():
                ohlcv_df = ohlcv_df_dict[symbol_type][timeframe][symbol]
                
                first_signal_time = start_date - dt.timedelta(minutes=interval_dict[timeframe])
                ohlcv_df = ohlcv_df[ohlcv_df['time'] >= first_signal_time].dropna().reset_index(drop=True)
                ohlcv_df_dict[symbol_type][timeframe][symbol] = ohlcv_df

    return ohlcv_df_dict
        

def add_signal(start_date, ohlcv_df_dict, interval_dict, config_params):
    func_add_dict = {
        'sma': add_sma,
        'ema': add_ema,
        'tma': add_tma,
        'supertrend': add_supertrend,
        'wt': add_wt,
        'rsi': add_rsi
    }

    ohlcv_df_dict = add_action_signal(ohlcv_df_dict, func_add_dict, config_params)
    ohlcv_df_dict = add_stop_signal(ohlcv_df_dict, func_add_dict, config_params)
    ohlcv_df_dict = filter_start_time(start_date, ohlcv_df_dict, interval_dict)

    return ohlcv_df_dict


def update_max_drawdown(symbol, side, close_price, max_drawdown, ohlcv_df, position_dict):
    if side == 'buy':
        low_price = close_price if close_price != None else ohlcv_df.loc[0, 'low']
        drawdown = (position_dict[symbol]['open_price'] - low_price) / position_dict[symbol]['open_price']
    elif side == 'sell':
        high_price = close_price if close_price != None else ohlcv_df.loc[0, 'high']
        drawdown = (high_price - position_dict[symbol]['open_price']) / position_dict[symbol]['open_price']
        
    if drawdown > max_drawdown:
        max_drawdown = drawdown

    return max_drawdown


def get_action_base(symbol, objective, action_list, signal_time, config_params, ohlcv_df_dict):
    for timeframe in config_params['base'][objective].keys():
        base_ohlcv_df = ohlcv_df_dict['base'][timeframe][symbol]

        for signal in config_params['base'][objective][timeframe].keys():
            for func_check in config_params['base'][objective][timeframe][signal]['check']:
                action_side = func_check(objective, 'base', signal_time, signal, action_list, base_ohlcv_df, timeframe, config_params)
                func_name = str(func_check).split(' ')[1]
                print(f"     base {symbol} {func_name} {signal} {timeframe}: {action_side}")

    return action_list


def get_action_lead(objective, action_list, signal_time, config_params, ohlcv_df_dict):
    for timeframe in config_params['lead'][objective].keys():
        for lead_symbol in config_params['lead']['symbol']:
            lead_ohlcv_df = ohlcv_df_dict['lead'][timeframe][lead_symbol]

            for signal in config_params['lead'][objective][timeframe].keys():
                for func_check in config_params['lead'][objective][timeframe][signal]['check']:
                    action_side = func_check(objective, 'lead', signal_time, signal, action_list, lead_ohlcv_df, timeframe, config_params)
                    func_name = str(func_check).split(' ')[1]
                    print(f"     lead {lead_symbol} {func_name} {signal} {timeframe}: {action_side}")

    return action_list


def get_action(symbol, objective, action_list, signal_time, config_params, ohlcv_df_dict):
    action_list = get_action_base(symbol, objective, action_list, signal_time, config_params, ohlcv_df_dict)
    action_list = get_action_lead(objective, action_list, signal_time, config_params, ohlcv_df_dict)    

    return action_list


def get_available_data_flag(symbol, signal_time, max_open_timeframe, ohlcv_df_dict):
    ohlcv_df = ohlcv_df_dict['base'][max_open_timeframe][symbol]

    # Index 0 is used for signal check, First timestamp start at index 1.
    if ohlcv_df.loc[1, 'time'] <= signal_time:
        available_data_flag = True
    else:
        available_data_flag = False

    return available_data_flag


def get_open_position_flag(symbol, signal_time, max_open_timeframe, config_params, ohlcv_df_dict):
    available_data_flag = get_available_data_flag(symbol, signal_time, max_open_timeframe, ohlcv_df_dict)    
    
    if available_data_flag == True:
        action_list = []
        action_list = get_action(symbol, 'open', action_list, signal_time, config_params, ohlcv_df_dict)

        if (len(set(action_list)) == 1) & (action_list[0] != 'no_action'):
            open_position_flag = True
            side = action_list[0]
        else:
            open_position_flag = False
            side = None
            print(f"     No action")
    else:
        open_position_flag = False
        side = None
        print(f"     Not available data")

    return open_position_flag, side


def get_close_position_flag(symbol, side, signal_time, config_params, current_ohlcv_df, ohlcv_df_dict, position_dict):
    if ((side == 'buy') & (current_ohlcv_df.loc[0, 'high'] >= position_dict[symbol]['tp'])) | ((side == 'sell') & (current_ohlcv_df.loc[0, 'low'] <= position_dict[symbol]['tp'])):
        close_position_flag = True
        close_price = position_dict[symbol]['tp']
        print(f"     Closed by tp at {close_price}")
    elif ((side == 'buy') & (current_ohlcv_df.loc[0, 'low'] <= position_dict[symbol]['sl'])) | ((side == 'sell') & (current_ohlcv_df.loc[0, 'high'] >= position_dict[symbol]['sl'])):
        close_position_flag = True
        close_price = position_dict[symbol]['sl']
        print(f"     Closed by sl at {close_price}")
    else:
        action_list = [side]
        action_list = get_action(symbol, 'close', action_list, signal_time, config_params, ohlcv_df_dict)

        if (len(set(action_list)) != 1) | (action_list[0] != position_dict[symbol]['side']):
            close_position_flag = True
            close_price = current_ohlcv_df.loc[0, 'close']
            print(f"     Closed by signal at {close_price}")
        else:
            close_position_flag = False
            close_price = None
            print("     Not close")

    return close_position_flag, close_price


def update_open_opsition(symbol, side, open_price, amount, tp_price, sl_price, signal_time, position_dict, config_params, interval_dict):
    action_time = signal_time + dt.timedelta(minutes=interval_dict[config_params['action_timeframe']])

    position_dict[symbol] = {
        'side': side,
        'open_time': action_time,
        'open_price': open_price,
        'amount': amount,
        'notional': amount * open_price,
        'tp': tp_price,
        'sl': sl_price
    }

    print(f"     {side}: {amount}")
    print(f"     price: {position_dict[symbol]['open_price']}")
    print(f"     tp: {position_dict[symbol]['tp']}")
    print(f"     sl: {position_dict[symbol]['sl']}")

    return position_dict


def update_close_position(symbol, side, close_price, signal_time, config_params, budget, reinvest_profit_flag, position_dict, transaction_dict, interval_dict):
    action_time = signal_time + dt.timedelta(minutes=interval_dict[config_params['action_timeframe']])

    transaction_dict['symbol'].append(symbol)
    transaction_dict['side'].append(side)
    transaction_dict['amount'].append(position_dict[symbol]['amount'])
    transaction_dict['open_time'].append(position_dict[symbol]['open_time'])
    transaction_dict['open_price'].append(position_dict[symbol]['open_price'])
    transaction_dict['close_time'].append(action_time)
    transaction_dict['close_price'].append(close_price)
    transaction_dict['notional'].append(position_dict[symbol]['notional'])

    if position_dict[symbol]['side'] == 'buy':
        adjusted_open_price = position_dict[symbol]['open_price'] * (1 + (config_params['taker_fee'] / 100))
        adjusted_close_price = close_price * (1 - (config_params['taker_fee'] / 100))
        profit = (adjusted_close_price - adjusted_open_price) * position_dict[symbol]['amount']
        profit_percent = ((adjusted_close_price - adjusted_open_price) / adjusted_open_price) * 100
    elif position_dict[symbol]['side'] == 'sell':
        adjusted_open_price = position_dict[symbol]['open_price'] * (1 - (config_params['taker_fee'] / 100))
        adjusted_close_price = close_price * (1 + (config_params['taker_fee'] / 100))
        profit = (adjusted_open_price - adjusted_close_price) * position_dict[symbol]['amount']
        profit_percent = ((adjusted_open_price - adjusted_close_price) / adjusted_open_price) * 100

    transaction_dict['profit'].append(profit)
    transaction_dict['profit_percent'].append(profit_percent)
    
    if reinvest_profit_flag == True:
        budget += profit

    del position_dict[symbol]

    return budget, position_dict, transaction_dict


def open_position(symbol, signal_time, max_open_timeframe, config_params, budget, ohlcv_df_dict, position_dict, interval_dict):
    open_position_flag, side = get_open_position_flag(symbol, signal_time, max_open_timeframe, config_params, ohlcv_df_dict)

    if open_position_flag == True:
        ohlcv_df = ohlcv_df_dict['base'][config_params['action_timeframe']][symbol]
        current_ohlcv_df = ohlcv_df[ohlcv_df['time'] == signal_time].reset_index(drop=True)
        
        open_price = current_ohlcv_df.loc[0, 'close']
        amount = (config_params['action_percent'] * budget) / open_price * config_params['leverage']
        tp_price = get_stop_price('tp', side, symbol, signal_time, open_price, ohlcv_df_dict, config_params)
        sl_price = get_stop_price('sl', side, symbol, signal_time, open_price, ohlcv_df_dict, config_params)

        position_dict = update_open_opsition(symbol, side, open_price, amount, tp_price, sl_price, signal_time, position_dict, config_params, interval_dict)

    return position_dict


def close_position(symbol, signal_time, max_drawdown, config_params, budget, reinvest_profit_flag, ohlcv_df_dict, position_dict, transaction_dict, interval_dict):
    side = position_dict[symbol]['side']
    ohlcv_df = ohlcv_df_dict['base'][config_params['action_timeframe']][symbol]
    current_ohlcv_df = ohlcv_df[ohlcv_df['time'] == signal_time].reset_index(drop=True)
    
    close_position_flag, close_price = get_close_position_flag(symbol, side, signal_time, config_params, current_ohlcv_df, ohlcv_df_dict, position_dict)
    max_drawdown = update_max_drawdown(symbol, side, close_price, max_drawdown, current_ohlcv_df, position_dict)

    if close_position_flag == True:
        budget, position_dict, transaction_dict = update_close_position(symbol, side, close_price, signal_time, config_params, budget, reinvest_profit_flag, position_dict, transaction_dict, interval_dict)
    else:
        position_dict = update_stop_price(side, symbol, signal_time, config_params, position_dict, ohlcv_df_dict)

    return budget, max_drawdown, position_dict, transaction_dict