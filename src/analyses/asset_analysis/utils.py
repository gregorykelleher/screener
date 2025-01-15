# analyses/utils.py

def map_commodity_symbol(incoming_symbol: str) -> str:
    """
    Maps an incoming commodity asset symbol to a valid symbol for TradingView.
    
    If the asset symbol is recognized as one having an invalid format for TradingView,
    the method returns the corrected symbol. Otherwise, it returns the incoming symbol.
    
    :param incoming_symbol: The asset symbol as received.
    :return: A valid asset symbol for TradingView.
    """
    COMMODITY_SYMBOL_MAPPING = {
        "ZTUSD": "CBOT:ZT1!",    # 2-Year T-Note Futures
        "ZNUSD": "CBOT:ZN1!",    # 10-Year T-Note Futures
        "ALIUSD": "COMEX:ALI1!", # Aluminum Futures
        "HGUSD": "COMEX:HG1!",   # Copper Futures
        "GCUSD": "COMEX:GC1!",   # Gold Futures
        "SIUSD": "COMEX:SI1!",   # Silver Futures
        "RBUSD": "NYMEX:RB1!",   # Gasoline RBOB Futures
        "CLUSD": "NYMEX:CL1!",   # Crude Oil Futures
        "NGUSD": "NYMEX:NG1!",   # Natural Gas Futures
        "BZUSD": "NYMEX:BB1!",   # Brent Crude Oil Futures
        "KEUSX": "CBOT:KE1!",    # Wheat Futures
        "ZCUSX": "CBOT:ZC1!",    # Corn Futures
        "LBUSD": "CME:LBR1!",    # Lumber Futures
        "ZOUSX": "CBOT:ZO1!",    # Oat Futures
        "KCUSX": "NYMEX:KT1!",   # Coffee Futures
    }
    
    return COMMODITY_SYMBOL_MAPPING.get(incoming_symbol, incoming_symbol)
