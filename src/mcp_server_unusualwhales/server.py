import os
import httpx
import inspect
import logging
import polars as pl
from mcp.server.fastmcp import FastMCP

uw_token = os.getenv('UNUSUAL_WHALES_API_TOKEN')
headers = {'Accept': 'application/json, text/plain', 'Authorization': uw_token}
mcp = FastMCP('UnusualWhales', dependencies=['polars', 'httpx'])

@mcp.tool()
def get_flow_alerts(
    all_opening: bool=False,
    is_ask_side: bool=False,
    is_bid_side: bool=False,
    is_call: bool=False,
    is_floor: bool=False,
    is_otm: bool=False,
    is_put: bool=False,
    is_sweep: bool=False,
    issue_types: list[str]=None,
    limit: int=200,
    max_diff: float=0.0,
    max_dte: int=0,
    max_open_interest: int=0,
    max_premium: int=0,
    max_size: int=0,
    max_volume: int=0,
    max_volume_oi_ratio: float=0.0,
    min_diff: float=0.0,
    min_dte: int=0,
    min_open_interest: int=0,
    min_premium: int=0,
    min_size: int=0,
    min_volume: int=0,
    min_volume_oi_ratio: float=0.0,
    newer_than: str=None,
    older_than: str=None,
    rule_name: list[str]=None,
    ticker_symbol: str=None
) -> pl.DataFrame:
    """
    Fetch Flow Alerts from the Unusual Whales API using the input parameters
    to filter the results. Returns the Flow Alerts in a Polars DataFrame.
    Args:
        all_opening (bool): If True, only include trades where every transaction was an opening trade (very rare)
        is_ask_side (bool): If true, only include trades on the ask side
        is_bid_side (bool): If true, only include trades on the bid side
        is_call (bool): If true, only include call trades
        is_floor (bool): If true, only include floor-executed trades
        is_otm (bool): If true, only include out-of-the-money trades
        is_put (bool): If true, only include put trades
        is_sweep (bool): If true, only include intermarket sweep trades
        issue_types (list[str]): List, allowed values are 'Common Stock', 'ETF', 'Index', 'ADR'
        limit (int): Maximum number of results to return (200 is maximum)
        max_diff (float): Maximum difference (as a decimal) between strike and underlying
        max_dte (int): Maximum days to expiration
        max_open_interest (int): Maximum open interest for the option
        max_premium (int): Maximum premium for the option
        max_size (int): Maximum number of contracts in the trade
        max_volume (int): Maximum volume for the option
        max_volume_oi_ratio (float): Maximum volume to open interest ratio
        min_diff (float): Minimum difference (as a decimal) between strike and underlying
        min_dte (int): Minimum days to expiration
        min_open_interest (int): Minimum open interest for the option
        min_premium (int): Minimum premium for the option
        min_size (int): Minimum number of contracts in the trade
        min_volume (int): Minimum volume for the option
        min_volume_oi_ratio (float): Minimum volume to open interest ratio
        newer_than (str): ISO 8601 formatted date string for filtering results
        older_than (str): ISO 8601 formatted date string for filtering results
        rule_name (list[str]): List, allowed values are 'FloorTradeSmallCap', 'FloorTradeMidCap', 'RepeatHits', 'RepeatedHitsAscendingFill', 'RepeatedHitsDescendingFill', 'FloorTradeLargeCap', 'OtmEarningsFloor', 'LowHistoricVolumeFloor', 'SweepsFollowedByFloor'
        ticker_symbol (str): Ticker symbol, for only AAPL and INTC use 'AAPL,INTC' and to exclude AAPL and INTC use '-AAPL,INTC'
    """
    url = 'https://api.unusualwhales.com/api/option-trades/flow-alerts'

    default_values = get_flow_alerts.__defaults__
    param_names = get_flow_alerts.__code__.co_varnames[:get_flow_alerts.__code__.co_argcount]
    defaults = dict(zip(param_names, default_values))
    
    # Build query params, including only explicitly passed parameters
    frame = inspect.currentframe()
    args = frame.f_locals
    params = {
        name: args[name] 
        for name in param_names 
        if name in args
        and (args[name] is not None)
        and (name not in defaults or args[name] != defaults[name])
        and name not in ('url', 'frame')
    }
    del frame

    try:
        with httpx.Client(timeout=30.0) as client:
            rsp = client.get(url, params=params, headers=headers)
            rsp.raise_for_status()
            
            data = rsp.json()['data']
            if not data:
                return pl.DataFrame()
            else:
                df = pl.DataFrame(data)
                return (
                    df
                    .with_columns(
                        pl.col('created_at').cast(pl.Datetime),
                        pl.col('expiry').cast(pl.Date),
                        pl.col('next_earnings_date').cast(pl.Date),
                        pl.col('total_ask_side_prem').cast(pl.Int64),
                        pl.col('total_bid_side_prem').cast(pl.Int64),
                        pl.col('total_premium').cast(pl.Int64),
                        pl.col('ask').cast(pl.Decimal),
                        pl.col('bid').cast(pl.Decimal),
                        pl.col('iv_end').cast(pl.Decimal),
                        pl.col('iv_start').cast(pl.Decimal),
                        pl.col('marketcap').cast(pl.Decimal),
                        pl.col('price').cast(pl.Decimal),
                        pl.col('strike').cast(pl.Decimal),
                        pl.col('underlying_price').cast(pl.Decimal),
                        pl.col('volume_oi_ratio').cast(pl.Decimal),
                    )
                    .with_columns(
                        pl.col('created_at').dt.convert_time_zone('America/New_York')
                    )
                )

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise ValueError('Invalid or missing API key')
        elif e.response.status_code == 404:
            raise ValueError(f'Resource not found: {e.response.text}')
        elif e.response.status_code == 429:
            raise ValueError('Rate limit exceeded')
        else:
            raise ValueError(f'HTTP error: {e.response.status_code} - {e.response.text}')
    
    except httpx.RequestError as e:
        raise ConnectionError(f'Network error: {e.request.url} - {str(e)}')
    
    except httpx.TimeoutException:
        raise TimeoutError('Request timed out')
