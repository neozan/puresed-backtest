import numpy as np
import datetime as dt

from func_signal import call_check_signal_func


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
    for timeframe in config_params['base'][objective]:
        base_ohlcv_df = ohlcv_df_dict['base'][timeframe][symbol]

        for signal in config_params['base'][objective][timeframe]:
            for func_name in config_params['base'][objective][timeframe][signal]['check']:
                action_side = call_check_signal_func(func_name)(objective, 'base', signal_time, signal, action_list, base_ohlcv_df, timeframe, config_params)
                print(f"     base {symbol} {func_name} {signal} {timeframe}: {action_side}")

    return action_list


def get_action_lead(objective, action_list, signal_time, config_params, ohlcv_df_dict):
    for timeframe in config_params['lead'][objective]:
        for lead_symbol in config_params['lead']['symbol']:
            lead_ohlcv_df = ohlcv_df_dict['lead'][timeframe][lead_symbol]

            for signal in config_params['lead'][objective][timeframe]:
                for func_name in config_params['lead'][objective][timeframe][signal]['check']:
                    action_side = call_check_signal_func(func_name)(objective, 'lead', signal_time, signal, action_list, lead_ohlcv_df, timeframe, config_params)
                    print(f"     lead {lead_symbol} {func_name} {signal} {timeframe}: {action_side}")

    return action_list


def get_action(symbol, objective, action_list, signal_time, config_params, ohlcv_df_dict):
    action_list = get_action_base(symbol, objective, action_list, signal_time, config_params, ohlcv_df_dict)
    action_list = get_action_lead(objective, action_list, signal_time, config_params, ohlcv_df_dict)    

    return action_list


def get_max_open_timeframe(config_params, interval_dict):
    timeframe_list = list(config_params['base']['open'])
    interval_list = [interval_dict[timeframe] for timeframe in timeframe_list]
    max_interval = max(interval_list)
    
    inverse_dict = {v: k for k, v in interval_dict.items()}
    max_timeframe = inverse_dict[max_interval]
    
    return max_timeframe


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
    
    if available_data_flag:
        action_list = []
        action_list = get_action(symbol, 'open', action_list, signal_time, config_params, ohlcv_df_dict)

        if (len(set(action_list)) == 1) & (action_list[0] in config_params['target_side']) & (action_list[0] != 'no_action'):
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


def get_tp_flag(symbol, side, current_ohlcv_df, position_dict):
    if (side == 'buy') & (current_ohlcv_df.loc[0, 'high'] >= position_dict[symbol]['tp']):
        tp_flag = True
    elif (side == 'sell') & (current_ohlcv_df.loc[0, 'low'] <= position_dict[symbol]['tp']):
        tp_flag = True
    else:
        tp_flag = False

    return tp_flag


def get_sl_flag(symbol, side, current_ohlcv_df, position_dict):
    print(f"       side: {side}")
    print(f"       price: {current_ohlcv_df.loc[0, 'low']}")
    print(f"       sl: {position_dict[symbol]['sl']}")

    if (side == 'buy') & (current_ohlcv_df.loc[0, 'low'] <= position_dict[symbol]['sl']):
        sl_flag = True
    elif (side == 'sell') & (current_ohlcv_df.loc[0, 'high'] >= position_dict[symbol]['sl']):
        sl_flag = True
    else:
        sl_flag = False

    print(f"       sl_flag: {sl_flag}")

    return sl_flag


def get_close_position_flag(symbol, side, signal_time, config_params, current_ohlcv_df, ohlcv_df_dict, position_dict):
    if (position_dict[symbol]['stop_count'] == 0) & (get_tp_flag(symbol, side, current_ohlcv_df, position_dict)):
        close_position_flag = True
        close_price = position_dict[symbol]['tp']
        close_percent = config_params['tp']['stop_percent']
        print(f"     Take profit at {close_price}")
    elif (position_dict[symbol]['stop_count'] == 0) & (get_sl_flag(symbol, side, current_ohlcv_df, position_dict)):
        close_position_flag = True
        close_price = position_dict[symbol]['sl']
        close_percent = config_params['sl']['stop_percent']
        print(f"     Stop loss at {close_price}")
    else:
        action_list = [side]
        action_list = get_action(symbol, 'close', action_list, signal_time, config_params, ohlcv_df_dict)

        if (len(set(action_list)) != 1) | (action_list[0] != position_dict[symbol]['side']):
            close_position_flag = True
            close_price = current_ohlcv_df.loc[0, 'close']
            close_percent = 100
            print(f"     Closed by signal at {close_price}")
        else:
            close_position_flag = False
            close_price = None
            close_percent = None
            print("     Not close")

    return close_position_flag, close_price, close_percent


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

        signal = list(config_params[stop_key]['signal']['signal'])[0]
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


def update_open_opsition(symbol, side, open_price, amount, tp_price, sl_price, signal_time, position_dict, config_params, interval_dict):
    action_time = signal_time + dt.timedelta(minutes=interval_dict[config_params['action_timeframe']])

    position_dict[symbol] = {
        'side': side,
        'open_time': action_time,
        'open_price': open_price,
        'amount': amount,
        'notional': amount * open_price,
        'tp': tp_price,
        'sl': sl_price,
        'stop_count': 0
    }

    print(f"     {side}: {amount}")
    print(f"     price: {position_dict[symbol]['open_price']}")
    print(f"     tp: {position_dict[symbol]['tp']}")
    print(f"     sl: {position_dict[symbol]['sl']}")

    return position_dict


def update_close_position(symbol, side, close_price, close_percent, signal_time, config_params, budget, reinvest_profit_flag, position_dict, transaction_dict, interval_dict):
    action_time = signal_time + dt.timedelta(minutes=interval_dict[config_params['action_timeframe']])

    close_amount = position_dict[symbol]['amount'] * (close_percent / 100)

    transaction_dict['symbol'].append(symbol)
    transaction_dict['side'].append(side)
    transaction_dict['amount'].append(close_amount)
    transaction_dict['open_time'].append(position_dict[symbol]['open_time'])
    transaction_dict['open_price'].append(position_dict[symbol]['open_price'])
    transaction_dict['close_time'].append(action_time)
    transaction_dict['close_price'].append(close_price)
    transaction_dict['value'].append(position_dict[symbol]['open_price'] * close_amount)
    transaction_dict['notional'].append(position_dict[symbol]['notional'])

    if position_dict[symbol]['side'] == 'buy':
        adjusted_open_price = position_dict[symbol]['open_price'] * (1 + (config_params['taker_fee_percent'] / 100))
        adjusted_close_price = close_price * (1 - (config_params['taker_fee_percent'] / 100))
        profit = close_amount * (adjusted_close_price - adjusted_open_price)
        profit_percent = ((adjusted_close_price - adjusted_open_price) / adjusted_open_price) * 100
    elif position_dict[symbol]['side'] == 'sell':
        adjusted_open_price = position_dict[symbol]['open_price'] * (1 - (config_params['taker_fee_percent'] / 100))
        adjusted_close_price = close_price * (1 + (config_params['taker_fee_percent'] / 100))
        profit = close_amount * (adjusted_open_price - adjusted_close_price)
        profit_percent = ((adjusted_open_price - adjusted_close_price) / adjusted_open_price) * 100

    transaction_dict['profit'].append(profit)
    transaction_dict['profit_percent'].append(profit_percent)
    
    if reinvest_profit_flag:
        budget += profit

    position_dict[symbol]['amount'] -= close_amount

    if position_dict[symbol]['amount'] == 0:
        del position_dict[symbol]
    else:
        position_dict[symbol]['notional'] -= (close_amount * position_dict[symbol]['open_price'])
        position_dict[symbol]['stop_count'] = 1

    return budget, position_dict, transaction_dict


def update_stop_price(side, symbol, signal_time, config_params, position_dict, ohlcv_df_dict):
    for stop_key in ['tp', 'sl']:
        stop_price = get_stop_price(stop_key, side, symbol, signal_time, position_dict[symbol]['open_price'], ohlcv_df_dict, config_params)
            
        if position_dict[symbol][stop_key] != stop_price:
            position_dict[symbol][stop_key] = stop_price
            print(f"     Update {stop_key}: {stop_price}")

    return position_dict


def open_position(symbol, signal_time, max_open_timeframe, config_params, budget, ohlcv_df_dict, position_dict, interval_dict):
    open_position_flag, side = get_open_position_flag(symbol, signal_time, max_open_timeframe, config_params, ohlcv_df_dict)

    if open_position_flag:
        ohlcv_df = ohlcv_df_dict['base'][config_params['action_timeframe']][symbol]
        current_ohlcv_df = ohlcv_df[ohlcv_df['time'] == signal_time].reset_index(drop=True)
        
        open_price = current_ohlcv_df.loc[0, 'close']
        amount = ((config_params['action_percent'] / 100) * budget) / open_price * config_params['leverage']
        tp_price = get_stop_price('tp', side, symbol, signal_time, open_price, ohlcv_df_dict, config_params)
        sl_price = get_stop_price('sl', side, symbol, signal_time, open_price, ohlcv_df_dict, config_params)

        position_dict = update_open_opsition(symbol, side, open_price, amount, tp_price, sl_price, signal_time, position_dict, config_params, interval_dict)

    return position_dict


def close_position(symbol, signal_time, max_drawdown, config_params, budget, reinvest_profit_flag, ohlcv_df_dict, position_dict, transaction_dict, interval_dict):
    side = position_dict[symbol]['side']
    ohlcv_df = ohlcv_df_dict['base'][config_params['action_timeframe']][symbol]
    current_ohlcv_df = ohlcv_df[ohlcv_df['time'] == signal_time].reset_index(drop=True)
    
    close_position_flag, close_price, close_percent = get_close_position_flag(symbol, side, signal_time, config_params, current_ohlcv_df, ohlcv_df_dict, position_dict)
    max_drawdown = update_max_drawdown(symbol, side, close_price, max_drawdown, current_ohlcv_df, position_dict)

    if close_position_flag:
        budget, position_dict, transaction_dict = update_close_position(symbol, side, close_price, close_percent, signal_time, config_params, budget, reinvest_profit_flag, position_dict, transaction_dict, interval_dict)
    else:
        position_dict = update_stop_price(side, symbol, signal_time, config_params, position_dict, ohlcv_df_dict)

    return budget, max_drawdown, position_dict, transaction_dict