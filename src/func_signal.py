import numpy as np
import pandas as pd
import math


def get_signal_dict(signal, objective, timeframe, config_params):
    if objective in ['open', 'close']:
        signal_dict = config_params['base'][objective][timeframe][signal]
    elif objective in ['tp', 'sl']:
        signal_dict = config_params[objective]['signal']['signal'][signal]

    return signal_dict


def get_signal_side(df, signal):
    if df['close'] > df[signal]:
        signal_side = 'buy'
    elif df['close'] < df[signal]:
        signal_side = 'sell'
    else:
        signal_side = None
        
    return signal_side


def revert_signal(action_side):
    if action_side == 'buy':
        action_side = 'sell'
    elif action_side == 'sell':
        action_side = 'buy'

    return action_side
    

def add_sma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('sma', objective, timeframe, config_params)
    windows = signal_dict['windows']
    
    temp_df = ohlcv_df.copy()

    sma_list = temp_df['close'].rolling(window=windows).mean()
    ohlcv_df['sma'] = sma_list
    ohlcv_df['sma_side'] = ohlcv_df.apply(get_signal_side, signal='sma', axis=1)
    
    return ohlcv_df


def add_ema(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('ema', objective, timeframe, config_params)
    windows = signal_dict['windows']
    
    temp_df = ohlcv_df.copy()

    ema_list = temp_df['close'].ewm(span=windows, adjust=False).mean()
    ohlcv_df['ema'] = ema_list
    ohlcv_df['ema_side'] = ohlcv_df.apply(get_signal_side, signal='ema', axis=1)
    
    return ohlcv_df


def add_tma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('tma', objective, timeframe, config_params)
    windows = signal_dict['windows']
    sub_interval = (windows + 1) / 2

    temp_df = ohlcv_df.copy()
    
    sma_list = temp_df['close'].rolling(window=math.trunc(sub_interval)).mean()
    temp_df['ma'] = sma_list
    
    tma_list = temp_df['ma'].rolling(window=int(np.round(sub_interval))).mean()
    ohlcv_df['tma'] = tma_list
    ohlcv_df['tma_side'] = ohlcv_df.apply(get_signal_side, signal='tma', axis=1)
    
    return ohlcv_df


def add_bollinger(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('bollinger', objective, timeframe, config_params)
    windows = signal_dict['windows']
    
    temp_df = ohlcv_df.copy()

    sma_list = temp_df['close'].rolling(window=windows).mean()
    std_list = temp_df['close'].rolling(window=windows).std(ddof=0)
    
    bollinger_upper = sma_list + (std_list * signal_dict['std'])
    bollinger_lower = sma_list - (std_list * signal_dict['std'])
    
    ohlcv_df['bollinger_upper'] = bollinger_upper
    ohlcv_df['bollinger_lower'] = bollinger_lower
    
    return ohlcv_df


def add_supertrend(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('supertrend', objective, timeframe, config_params)
    atr_range = signal_dict['atr_range']
    multiplier = signal_dict['multiplier']
    
    def add_atr(ohlcv_df, atr_range):
        temp_df = ohlcv_df.copy()

        high_low = temp_df['high'] - temp_df['low']
        high_close = np.abs(temp_df['high'] - temp_df['close'].shift())
        low_close = np.abs(temp_df['low'] - temp_df['close'].shift())

        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        temp_df['true_range'] = true_range
        ohlcv_df['atr'] = temp_df['true_range'].ewm(alpha=1 / atr_range, min_periods=atr_range).mean()
        ohlcv_df = ohlcv_df.dropna().reset_index(drop=True)

        return ohlcv_df
    
    ohlcv_df = add_atr(ohlcv_df, atr_range)
    temp_df = ohlcv_df.copy()
    
    supertrend_list = []
    supertrend_side_list = []
    final_upperband_list = []
    final_lowerband_list = []

    for i in range(len(temp_df)):
        mid_price = (temp_df.loc[i, 'high'] + temp_df.loc[i, 'low']) / 2
        default_atr = multiplier * temp_df.loc[i, 'atr']
        basic_upperband = mid_price + default_atr
        basic_lowerband = mid_price - default_atr

        try:
            if (basic_upperband < final_upperband_list[i - 1]) | (temp_df.loc[i - 1, 'close'] > final_upperband_list[i - 1]):
                final_upperband = basic_upperband
            else:
                final_upperband = final_upperband_list[i - 1]
                
            if (basic_lowerband > final_lowerband_list[i - 1]) | (temp_df.loc[i - 1, 'close'] < final_lowerband_list[i - 1]):
                final_lowerband = basic_lowerband
            else:
                final_lowerband = final_lowerband_list[i - 1]
        except IndexError:
            # First loop
            final_upperband = basic_upperband
            final_lowerband = basic_lowerband
            
        final_upperband_list.append(final_upperband)
        final_lowerband_list.append(final_lowerband)
            
        if temp_df.loc[i, 'close'] > final_upperband:
            supertrend = final_lowerband
            supertrend_side = 'buy'
        elif temp_df.loc[i, 'close'] < final_lowerband:
            supertrend = final_upperband
            supertrend_side = 'sell'
        else:
            try:
                supertrend_side = supertrend_side_list[-1]
            except IndexError:
                # First loop
                supertrend_side = None
                
            if supertrend_side == 'buy':
                supertrend = final_lowerband
            else:
                supertrend = final_upperband
        
        supertrend_side_list.append(supertrend_side)
        supertrend_list.append(supertrend)

    ohlcv_df['supertrend'] = supertrend_list
    ohlcv_df['supertrend_side'] = supertrend_side_list
    ohlcv_df = ohlcv_df.drop(columns=['atr'])
    
    return ohlcv_df


def add_wt(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('wt', objective, timeframe, config_params)
    channel_range = signal_dict['channel_range']
    average_range = signal_dict['average_range']
    
    temp_df = ohlcv_df.copy()
    
    temp_df['average_price'] = (temp_df['high'] + temp_df['low'] + temp_df['close']) / 3
    temp_df['esa'] = temp_df['average_price'].ewm(span=channel_range).mean()
    temp_df['dd'] = abs(temp_df['average_price'] - temp_df['esa'])
    temp_df['d'] = temp_df['dd'].ewm(span=channel_range).mean()
    temp_df['ci'] = (temp_df['average_price'] - temp_df['esa']) / (0.015 * temp_df['d'])
    
    wt_list = temp_df['ci'].ewm(span=average_range).mean()
    ohlcv_df['wt'] = wt_list
    
    return ohlcv_df


def add_rsi(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('rsi', objective, timeframe, config_params)
    windows = signal_dict['average_range']

    temp_df = ohlcv_df.copy()
    
    temp_df['diff'] = temp_df['close'].diff(1)
    temp_df['gain'] = temp_df['diff'].clip(lower=0)
    temp_df['loss'] = temp_df['diff'].clip(upper=0).abs()

    temp_df['avg_gain'] = temp_df['gain'].rolling(window=windows, min_periods=windows).mean()[:windows+1]
    temp_df['avg_loss'] = temp_df['loss'].rolling(window=windows, min_periods=windows).mean()[:windows+1]

    for i, _ in enumerate(temp_df.loc[windows + 1:, 'avg_gain']):
        temp_df.loc[i + windows + 1, 'avg_gain'] = (temp_df.loc[i + windows, 'avg_gain'] * (windows - 1) + temp_df.loc[i + windows + 1, 'gain']) / windows

    for i, _ in enumerate(temp_df.loc[windows + 1:, 'avg_loss']):
        temp_df.loc[i + windows + 1, 'avg_loss'] = (temp_df.loc[i + windows, 'avg_loss'] * (windows - 1) + temp_df.loc[i + windows + 1, 'loss']) / windows

    temp_df['rs'] = temp_df['avg_gain'] / temp_df['avg_loss']
    
    rsi_list = 100 - (100 / (1.0 + temp_df['rs']))
    ohlcv_df['rsi'] = rsi_list
  
    return ohlcv_df


def check_signal_price(objective, symbol_type, time, signal, action_list, ohlcv_df, timeframe, config_params):
    check_df = ohlcv_df[ohlcv_df['time'] <= time].reset_index(drop=True)
    check_series = check_df.loc[len(check_df) - 1, :]
    
    action_price = check_series[signal]

    if check_series['close'] > action_price:
        action_side = 'buy'
    elif check_series['close'] < action_price:
        action_side = 'sell'
    else:
        action_side = 'no_action'

    if config_params[symbol_type][objective][timeframe][signal]['revert'] == True:
        action_side = revert_signal(action_side)
    
    action_list.append(action_side)
    return action_side


def check_signal_side_change(objective, symbol_type, time, signal, action_list, ohlcv_df, timeframe, config_params):
    look_back = config_params[symbol_type][objective][timeframe][signal]['look_back']

    if look_back > 0:
        check_df = ohlcv_df[ohlcv_df['time'] <= time]
        check_df = check_df.loc[len(check_df) - look_back - 1:].reset_index(drop=True)
        
        if len(check_df) >= look_back + 1:
            if check_df.loc[0, f'{signal}_side'] != check_df.loc[len(check_df) - 1, f'{signal}_side']:
                action_side = check_df.loc[len(check_df) - 1, f'{signal}_side']
            else:
                action_side = 'no_action' if objective == 'open' else check_df.loc[len(check_df) - 1, f'{signal}_side']
        else:
            action_side = 'no_action'

        if config_params[symbol_type][objective][timeframe][signal]['revert'] == True:
            action_side = revert_signal(action_side)

        action_list.append(action_side)
        return action_side


def check_signal_band(objective, symbol_type, time, signal, action_list, ohlcv_df, timeframe, config_params):
    check_df = ohlcv_df[ohlcv_df['time'] <= time].reset_index(drop=True)
    check_series = check_df.loc[len(check_df) - 1, :]

    band_type_dict = {
        'signal': ['rsi', 'wt'],
        'price': ['bollinger']
    }

    if signal in band_type_dict['signal']:
        indicator = check_series[signal]
        upperband = config_params[symbol_type][objective][timeframe][signal]['overbought']
        lowerband = config_params[symbol_type][objective][timeframe][signal]['oversold']
    elif signal in band_type_dict['price']:
        indicator = check_series['close']
        upperband = check_series[f'{signal}_upper']
        lowerband = check_series[f'{signal}_lower']
    
    if config_params[symbol_type][objective][timeframe][signal]['trigger'] == 'outer':
        if indicator <= lowerband:
            action_side = 'buy'
        elif indicator >= upperband:
            action_side = 'sell'
        else:
            action_side = 'no_action'
    
    elif config_params[symbol_type][objective][timeframe][signal]['trigger'] == 'inner':
        if (len(action_list) >= 1):
            if (action_list[-1] == 'buy') & (indicator < upperband):
                action_side = 'buy'
            elif (action_list[-1] == 'buy') & (indicator >= upperband):
                action_side = 'sell'
            elif (action_list[-1] == 'sell') & (indicator > lowerband):
                action_side = 'sell'
            elif (action_list[-1] == 'sell') & (indicator <= lowerband):
                action_side = 'buy'
            else:
                action_side = 'no_action'
        else:
            raise ValueError("Must be used with other signals")

    if config_params[symbol_type][objective][timeframe][signal]['revert'] == True:
        action_side = revert_signal(action_side)
        
    action_list.append(action_side)
    return action_side