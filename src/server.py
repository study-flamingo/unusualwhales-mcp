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


@mcp.tool()
def get_ticker_info(ticker: str) -> pl.DataFrame:
    """
    Fetch general information about a given stock ticker from the Unusual Whales API.
    Args:
        ticker (str): The stock ticker symbol (e.g., AAPL).
    Returns:
        pl.DataFrame: A Polars DataFrame containing information about the ticker.
    """
    url = f'https://api.unusualwhales.com/api/stock/{ticker}/info'
    
    try:
        with httpx.Client(timeout=30.0) as client:
            rsp = client.get(url, headers=headers)
            rsp.raise_for_status()
            
            data = rsp.json().get('data')
            if not data:
                return pl.DataFrame()
            else:
                # Ensure data is a list for DataFrame creation
                if not isinstance(data, list):
                    data = [data]
                df = pl.DataFrame(data)
                # Apply type conversions as needed based on expected schema
                # Example conversions (adjust based on actual data structure):
                if 'next_earnings_date' in df.columns:
                     df = df.with_columns(pl.col('next_earnings_date').cast(pl.Date, strict=False))
                if 'avg30_volume' in df.columns:
                     df = df.with_columns(pl.col('avg30_volume').cast(pl.Int64, strict=False))
                if 'marketcap' in df.columns:
                     df = df.with_columns(pl.col('marketcap').cast(pl.Decimal, strict=False))

                return df

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise ValueError('Invalid or missing API key')
        elif e.response.status_code == 404:
             # Check if the response body indicates 'not found' or is empty
            try:
                error_data = e.response.json()
                if not error_data or error_data.get('message') == 'Not Found': # Adjust based on actual error response
                     logging.warning(f"Ticker info not found for {ticker}. Returning empty DataFrame.")
                     return pl.DataFrame() # Return empty DataFrame for 404
                else:
                     raise ValueError(f'Resource not found: {e.response.text}')
            except Exception: # Fallback if response is not JSON or parsing fails
                 logging.warning(f"Ticker info not found for {ticker} (non-JSON 404 response). Returning empty DataFrame.")
                 return pl.DataFrame() # Return empty DataFrame for 404
        elif e.response.status_code == 429:
            raise ValueError('Rate limit exceeded')
        else:
            raise ValueError(f'HTTP error: {e.response.status_code} - {e.response.text}')
    
    except httpx.RequestError as e:
        raise ConnectionError(f'Network error: {e.request.url} - {str(e)}')
    
    except httpx.TimeoutException:
        raise TimeoutError('Request timed out')
    except Exception as e: # Catch other potential errors during processing
        logging.error(f"An unexpected error occurred in get_ticker_info for {ticker}: {e}")
        raise

@mcp.tool()
def get_stock_state(ticker: str) -> pl.DataFrame:
    """
    Fetch the last stock state (OHLCV) for a given ticker from the Unusual Whales API.
    Args:
        ticker (str): The stock ticker symbol (e.g., AAPL).
    Returns:
        pl.DataFrame: A Polars DataFrame containing the last stock state.
    """
    url = f'https://api.unusualwhales.com/api/stock/{ticker}/stock-state'
    
    try:
        with httpx.Client(timeout=30.0) as client:
            rsp = client.get(url, headers=headers)
            rsp.raise_for_status()
            
            data = rsp.json().get('data')
            if not data:
                return pl.DataFrame()
            else:
                 # Ensure data is a list for DataFrame creation
                if not isinstance(data, list):
                    data = [data]
                df = pl.DataFrame(data)
                # Apply type conversions
                return (
                    df
                    .with_columns(
                        pl.col('close').cast(pl.Decimal, strict=False),
                        pl.col('high').cast(pl.Decimal, strict=False),
                        pl.col('low').cast(pl.Decimal, strict=False),
                        pl.col('open').cast(pl.Decimal, strict=False),
                        pl.col('tape_time').cast(pl.Datetime, strict=False),
                        pl.col('total_volume').cast(pl.Int64, strict=False),
                        pl.col('volume').cast(pl.Int64, strict=False),
                    )
                     .with_columns(
                        pl.col('tape_time').dt.convert_time_zone('America/New_York')
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
    except Exception as e: # Catch other potential errors during processing
        logging.error(f"An unexpected error occurred in get_stock_state for {ticker}: {e}")
        raise

@mcp.tool()
def get_institution_holdings(
    name: str, 
    date: str = None, 
    start_date: str = None, 
    end_date: str = None, 
    security_types: list[str] = None, 
    limit: int = 500, 
    page: int = 0, 
    order: str = None, 
    order_direction: str = 'desc'
) -> pl.DataFrame:
    """
    Fetch holdings for a given institution from the Unusual Whales API.
    Args:
        name (str): The name or CIK of the institution (e.g., 'VANGUARD GROUP INC' or '0000102909').
        date (str): Optional market date (YYYY-MM-DD) to filter holdings for a specific report date.
        start_date (str): Optional start date (YYYY-MM-DD) for filtering report dates.
        end_date (str): Optional end date (YYYY-MM-DD) for filtering report dates.
        security_types (list[str]): Optional list of security types to filter by (e.g., ['Share', 'Call']).
        limit (int): Maximum number of results to return (default 500, max 500).
        page (int): Page number for pagination (starts at 0).
        order (str): Optional column to order results by (e.g., 'ticker', 'value').
        order_direction (str): Sort order ('asc' or 'desc', default 'desc').
    Returns:
        pl.DataFrame: A Polars DataFrame containing the institution's holdings.
    """
    url = f'https://api.unusualwhales.com/api/institution/{name}/holdings'
    
    default_values = get_institution_holdings.__defaults__
    param_names = get_institution_holdings.__code__.co_varnames[:get_institution_holdings.__code__.co_argcount]
    defaults = dict(zip(param_names, default_values))
    
    frame = inspect.currentframe()
    args = frame.f_locals
    params = {
        p_name: args[p_name] 
        for p_name in param_names 
        if p_name in args
        and (args[p_name] is not None)
        and (p_name not in defaults or args[p_name] != defaults[p_name])
        and p_name not in ('url', 'frame', 'name') # Exclude path param 'name'
    }
    del frame

    try:
        with httpx.Client(timeout=30.0) as client:
            rsp = client.get(url, params=params, headers=headers)
            rsp.raise_for_status()
            
            data = rsp.json().get('data')
            if not data:
                return pl.DataFrame()
            else:
                df = pl.DataFrame(data)
                # Apply type conversions based on the schema 'An Institution's Holdings'
                return (
                    df
                    .with_columns(
                        pl.col('avg_price').cast(pl.Decimal, strict=False).alias('avg_price'),
                        pl.col('close').cast(pl.Decimal, strict=False).alias('close'),
                        pl.col('date').cast(pl.Date, strict=False).alias('date'),
                        pl.col('first_buy').cast(pl.Date, strict=False).alias('first_buy'),
                        # historical_units is array, handle appropriately if needed later
                        pl.col('price_first_buy').cast(pl.Decimal, strict=False).alias('price_first_buy'),
                        pl.col('shares_outstanding').cast(pl.Decimal, strict=False).alias('shares_outstanding'),
                        pl.col('units').cast(pl.Int64, strict=False).alias('units'),
                        pl.col('units_change').cast(pl.Int64, strict=False).alias('units_change'),
                        pl.col('value').cast(pl.Int64, strict=False).alias('value'),
                    )
                )

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise ValueError('Invalid or missing API key')
        elif e.response.status_code == 404:
            raise ValueError(f'Resource not found for institution {name}: {e.response.text}')
        elif e.response.status_code == 429:
            raise ValueError('Rate limit exceeded')
        else:
            raise ValueError(f'HTTP error: {e.response.status_code} - {e.response.text}')
    
    except httpx.RequestError as e:
        raise ConnectionError(f'Network error: {e.request.url} - {str(e)}')
    
    except httpx.TimeoutException:
        raise TimeoutError('Request timed out')
    except Exception as e: # Catch other potential errors during processing
        logging.error(f"An unexpected error occurred in get_institution_holdings for {name}: {e}")
        raise

@mcp.tool()
def get_insider_transactions(
    ticker_symbol: str = None,
    min_value: str = None,
    max_value: str = None,
    min_price: str = None,
    max_price: str = None,
    owner_name: str = None,
    sectors: str = None,
    industries: str = None,
    min_marketcap: str = None,
    max_marketcap: str = None,
    market_cap_size: str = None,
    min_earnings_dte: str = None,
    max_earnings_dte: str = None,
    min_amount: str = None,
    max_amount: str = None,
    is_director: bool = None,
    is_officer: bool = None,
    is_s_p_500: bool = None,
    is_ten_percent_owner: bool = None,
    common_stock_only: bool = None,
    transaction_codes: list[str] = None,
    security_ad_codes: str = None,
    limit: int = 500,
    page: int = 0
) -> pl.DataFrame:
    """
    Fetch insider transactions from the Unusual Whales API, aggregated by default.
    Args:
        ticker_symbol (str): Optional comma-separated list of tickers (e.g., 'AAPL,MSFT', '-TSLA' to exclude).
        min_value (str): Minimum transaction value in dollars.
        max_value (str): Maximum transaction value in dollars.
        min_price (str): Minimum stock price at the time of transaction.
        max_price (str): Maximum stock price at the time of transaction.
        owner_name (str): Name of the insider who made the transaction.
        sectors (str): Filter by company sector(s) (comma-separated).
        industries (str): Filter by company industry or industries (comma-separated).
        min_marketcap (str): Minimum market capitalization.
        max_marketcap (str): Maximum market capitalization.
        market_cap_size (str): Size category of company market cap (small, mid, large).
        min_earnings_dte (str): Minimum days to earnings.
        max_earnings_dte (str): Maximum days to earnings.
        min_amount (str): Minimum number of shares in transaction.
        max_amount (str): Maximum number of shares in transaction.
        is_director (bool): Filter transactions by company directors.
        is_officer (bool): Filter transactions by company officers.
        is_s_p_500 (bool): Only include S&P 500 companies.
        is_ten_percent_owner (bool): Filter transactions by 10% owners.
        common_stock_only (bool): Only include common stock transactions.
        transaction_codes (list[str]): Filter by transaction codes (e.g., ['P', 'S']).
        security_ad_codes (str): Filter by security acquisition disposition codes (comma-separated).
        limit (int): Maximum number of results to return (default 500, max 500).
        page (int): Page number for pagination (starts at 0).
    Returns:
        pl.DataFrame: A Polars DataFrame containing insider transactions.
    """
    url = 'https://api.unusualwhales.com/api/insider/transactions'
    
    default_values = get_insider_transactions.__defaults__
    param_names = get_insider_transactions.__code__.co_varnames[:get_insider_transactions.__code__.co_argcount]
    defaults = dict(zip(param_names, default_values))
    
    frame = inspect.currentframe()
    args = frame.f_locals
    params = {
        p_name: args[p_name] 
        for p_name in param_names 
        if p_name in args
        and (args[p_name] is not None)
        and (p_name not in defaults or args[p_name] != defaults[p_name])
        and p_name not in ('url', 'frame')
    }
    # Handle list parameters correctly for query string
    if 'transaction_codes' in params and isinstance(params['transaction_codes'], list):
        params['transaction_codes[]'] = params.pop('transaction_codes')

    del frame

    try:
        with httpx.Client(timeout=30.0) as client:
            rsp = client.get(url, params=params, headers=headers)
            rsp.raise_for_status()
            
            data = rsp.json().get('data')
            if not data:
                return pl.DataFrame()
            else:
                df = pl.DataFrame(data)
                # Apply type conversions based on the schema 'Insider Trade Agg'
                return (
                    df
                    .with_columns(
                        pl.col('amount').cast(pl.Int64, strict=False),
                        pl.col('date_excercisable').cast(pl.Date, strict=False),
                        pl.col('expiration_date').cast(pl.Date, strict=False),
                        pl.col('filing_date').cast(pl.Date, strict=False),
                        # ids is array
                        pl.col('marketcap').cast(pl.Decimal, strict=False),
                        pl.col('next_earnings_date').cast(pl.Date, strict=False),
                        pl.col('price').cast(pl.Decimal, strict=False),
                        pl.col('price_excercisable').cast(pl.Decimal, strict=False),
                        pl.col('shares_owned_after').cast(pl.Int64, strict=False),
                        pl.col('shares_owned_before').cast(pl.Int64, strict=False),
                        pl.col('stock_price').cast(pl.Decimal, strict=False), # Assuming stock_price is decimal
                        pl.col('transaction_date').cast(pl.Date, strict=False),
                        pl.col('transactions').cast(pl.Int64, strict=False),
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
    except Exception as e: # Catch other potential errors during processing
        logging.error(f"An unexpected error occurred in get_insider_transactions: {e}")
        raise

@mcp.tool()
def get_congress_trades(limit: int = 100, date: str = None, ticker: str = None) -> pl.DataFrame:
    """
    Fetch recent trades reported by members of Congress from the Unusual Whales API.
    Args:
        limit (int): Maximum number of results to return (default 100, max 200).
        date (str): Optional market date (YYYY-MM-DD) to filter trades on or before this transaction date.
        ticker (str): Optional ticker symbol to filter trades by.
    Returns:
        pl.DataFrame: A Polars DataFrame containing recent Congress trades.
    """
    url = 'https://api.unusualwhales.com/api/congress/recent-trades'
    
    default_values = get_congress_trades.__defaults__
    param_names = get_congress_trades.__code__.co_varnames[:get_congress_trades.__code__.co_argcount]
    defaults = dict(zip(param_names, default_values))
    
    frame = inspect.currentframe()
    args = frame.f_locals
    params = {
        p_name: args[p_name] 
        for p_name in param_names 
        if p_name in args
        and (args[p_name] is not None)
        and (p_name not in defaults or args[p_name] != defaults[p_name])
        and p_name not in ('url', 'frame')
    }
    del frame

    try:
        with httpx.Client(timeout=30.0) as client:
            rsp = client.get(url, params=params, headers=headers)
            rsp.raise_for_status()
            
            data = rsp.json().get('data')
            if not data:
                return pl.DataFrame()
            else:
                df = pl.DataFrame(data)
                # Apply type conversions based on the schema 'Senate Stock'
                return (
                    df
                    .with_columns(
                        pl.col('filed_at_date').cast(pl.Date, strict=False),
                        pl.col('transaction_date').cast(pl.Date, strict=False),
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
    except Exception as e: # Catch other potential errors during processing
        logging.error(f"An unexpected error occurred in get_congress_trades: {e}")
        raise

@mcp.tool()
def get_news_headlines(
    sources: str = None, 
    search_term: str = None, 
    major_only: bool = False, 
    limit: int = 50, 
    page: int = 0
) -> pl.DataFrame:
    """
    Fetch recent financial news headlines from the Unusual Whales API.
    Args:
        sources (str): Optional comma-separated list of news sources to filter by (e.g., 'Reuters,Bloomberg').
        search_term (str): Optional term to filter headlines by content.
        major_only (bool): If True, only return major/significant news (default False).
        limit (int): Maximum number of results to return (default 50, max 100).
        page (int): Page number for pagination (starts at 0).
    Returns:
        pl.DataFrame: A Polars DataFrame containing news headlines.
    """
    url = 'https://api.unusualwhales.com/api/news/headlines'
    
    default_values = get_news_headlines.__defaults__
    param_names = get_news_headlines.__code__.co_varnames[:get_news_headlines.__code__.co_argcount]
    defaults = dict(zip(param_names, default_values))
    
    frame = inspect.currentframe()
    args = frame.f_locals
    params = {
        p_name: args[p_name] 
        for p_name in param_names 
        if p_name in args
        and (args[p_name] is not None)
        and (p_name not in defaults or args[p_name] != defaults[p_name])
        and p_name not in ('url', 'frame')
    }
    del frame

    try:
        with httpx.Client(timeout=30.0) as client:
            rsp = client.get(url, params=params, headers=headers)
            rsp.raise_for_status()
            
            data = rsp.json().get('data')
            if not data:
                return pl.DataFrame()
            else:
                df = pl.DataFrame(data)
                # Apply type conversions based on the schema 'Headline News'
                return (
                    df
                    .with_columns(
                        pl.col('created_at').cast(pl.Datetime, strict=False),
                        # meta is object, tags and tickers are arrays - handle if needed
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
    except Exception as e: # Catch other potential errors during processing
        logging.error(f"An unexpected error occurred in get_news_headlines: {e}")
        raise

