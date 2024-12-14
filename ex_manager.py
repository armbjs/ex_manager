# 필요 패키지 설치
# pip install redis apscheduler requests python-binance pybit

#############################################
# 상수 정의부 (최상단)
#############################################
REDIS_SERVER_ADDR = 'vultr-prod-3a00cfaa-7dc1-4fed-af5b-19dd1a19abf2-vultr-prod-cd4a.vultrdb.com'
REDIS_SERVER_PORT = '16752'
REDIS_SERVER_DB_INDEX = '0'
REDIS_SERVER_USERNAME = 'armbjs'
REDIS_SERVER_PASSWORD = 'xkqfhf12'
REDIS_SERVER_SSL = 'True'

REDIS_PUBLISH_CHANNEL_NAME_PREFIX = 'TEST_NEW_NOTICES'
REDIS_PUBLISH_BINANCE_CHANNEL = 'BINANCE_NEW_NOTICES'
REDIS_STREAM_KEY_BUY = 'fake_pubsub_massage:purchased_coins'

BINANCE_API_KEY_CR = "CUvOhge0SRuYgon2edHchbOVIX3d70GQCevrvRMPNsL5LJ1mTdtVKFfB3FDu9hay"
BINANCE_API_SECRET_CR = "rhHey3FnPHsoW1lhWgYmfdZmJJVkqTxL1kjBa6Y4TrBgV0AyNk2EIPqRaR8rNykf"

BINANCE_API_KEY_LILAC = "nn0r9JF9CjJ2vd1lsCbyqmmcmK5HWGX1jqL0ukuoZIJhDwRLy0Q1VOd2mLcee9Ur"
BINANCE_API_SECRET_LILAC = "8UkN9v0ZIhAaVd23lcp1IKbyj01WFCxSDkMR88lhRMp05I5qySVyoO9xDMUfkJuH"

BINANCE_API_KEY_EX = "F3UZITsSHTg45JaeDx50eaONy1eQixdHWETOOHQV9w19YzUMWebgJCTO0qtEH24Y"
BINANCE_API_SECRET_EX = "jqB0T9AAQbd4IMbATXGBqFGCIdiQHiO5G0e4WcOSNaUbBwDPM6dtiDirHa3qbfQf"

BYBIT_API_KEY = 'FzG8fr6fGbK5JFPhrq'
BYBIT_API_SECRET = '3Ign1vEI1Qj8Kx0B8ABBpjEnddicWEXQGRI3'

BITGET_API_KEY = "bg_7126c08570667fadf280eb381c4107c8"
BITGET_SECRET_KEY = "9a251d56cfc9ba75bb622b25d5b695baa5ac438fb2c4400711af296450a51706"
BITGET_PASSPHRASE = "wlthrwjrdmfh"
BITGET_BASE_URL = "https://api.bitget.com"


#############################################
# 메인 프로그램 코드
#############################################
try:
    import redis
    import json
    import time
    import os
    import sys
    import logging
    from apscheduler.schedulers.blocking import BlockingScheduler
    import math
    import requests
    import base64
    from urllib.parse import urlencode
    import hmac
    import hashlib
    import datetime

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    redis_client = redis.Redis(
        host=REDIS_SERVER_ADDR,
        port=int(REDIS_SERVER_PORT),
        db=int(REDIS_SERVER_DB_INDEX),
        username=REDIS_SERVER_USERNAME,
        password=REDIS_SERVER_PASSWORD,
        ssl=REDIS_SERVER_SSL,
        decode_responses=True
    )

    def publish_test_notices():
        current_time = int(time.time() * 1000)
        coin_symbol = f"TST{current_time % 1000:03d}"

        upbit_notice_en1 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"Market Support for {coin_symbol}(Tasdas), XRP(Ripple Network) (BTC, USDT Market)",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": "Trade",
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }

        try:
            upbit_notice_en1_json = json.dumps(upbit_notice_en1, ensure_ascii=False)
            redis_client.publish(REDIS_PUBLISH_CHANNEL_NAME_PREFIX, upbit_notice_en1_json)
            logger.info(f"Published UPBIT test notice for {coin_symbol}")
            logger.info(f"UPBIT notice data: {upbit_notice_en1_json}")
        except Exception as e:
            logger.error(f"Error publishing notices: {e}")

    def run_tests():
        logger.info("Executing test notices")
        publish_test_notices()

except Exception:
    pass


##############################################
# Binance
##############################################
try:
    from binance.client import Client
    from binance.enums import *

    binance_client_cr = Client(BINANCE_API_KEY_CR, BINANCE_API_SECRET_CR)
    binance_client_lilac = Client(BINANCE_API_KEY_LILAC, BINANCE_API_SECRET_LILAC)
    binance_client_ex = Client(BINANCE_API_KEY_EX, BINANCE_API_SECRET_EX)

    def get_spot_balance_for_client(client, account_name):
        print(f"\n=== Binance Spot Wallet Balance ({account_name}) ===")
        try:
            account_info = client.get_account()
            balances = [
                asset for asset in account_info['balances']
                if float(asset['free']) > 0 or float(asset['locked']) > 0
            ]
            if not balances:
                print("No balance.")
            for balance_item in balances:
                coin_name = balance_item['asset']
                free_amount = float(balance_item['free'])
                locked_amount = float(balance_item['locked'])
                print(f"{coin_name}: available: {free_amount}, locked: {locked_amount}")
        except Exception as e:
            print(f"Error ({account_name}): {e}")

    def get_spot_balance_all():
        get_spot_balance_for_client(binance_client_cr, "CR")
        get_spot_balance_for_client(binance_client_lilac, "LILAC")
        get_spot_balance_for_client(binance_client_ex, "EX")

    def buy_binance_coin_usdt_raw(client, coin, usdt_amount):
        try:
            coin = coin.upper()
            usdt_info = client.get_asset_balance(asset='USDT')
            if not usdt_info or float(usdt_info['free']) <= 0:
                return {"error": "No USDT balance"}

            usdt_balance = float(usdt_info['free'])
            if usdt_amount > usdt_balance:
                return {"error": "Insufficient USDT balance"}

            symbol = coin + "USDT"
            ticker = client.get_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])

            usdt_to_use = math.floor(usdt_amount * 100) / 100.0
            if usdt_to_use <= 0:
                return {"error": "Too small USDT amount"}

            quantity = usdt_to_use / price
            quantity = float(int(quantity))
            if quantity <= 0:
                return {"error": "Quantity too small"}

            order = client.create_order(
                symbol=symbol,
                side=SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )
            return order
        except Exception as e:
            return {"error": str(e)}

    def sell_all_binance_coin_raw(client, coin):
        try:
            coin = coin.upper()
            balance_info = client.get_asset_balance(asset=coin)
            if not balance_info:
                return {"error": "Balance query failed"}
            balance_amount = float(balance_info['free'])

            if balance_amount <= 0:
                return {"error": "No balance to sell"}

            quantity = float(int(balance_amount))
            if quantity <= 0:
                return {"error": "Quantity too small"}

            symbol = coin + "USDT"
            order = client.create_order(
                symbol=symbol,
                side=SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )
            return order
        except Exception as e:
            return {"error": str(e)}

    def get_recent_trades_raw(client, coin):
        try:
            symbol = (coin.upper() + "USDT")
            trades = client.get_my_trades(symbol=symbol, limit=200)
            return trades
        except Exception as e:
            return {"error": str(e)}

    def calculate_account_avg_buy_price(client, coin):
        trades = get_recent_trades_raw(client, coin)
        if isinstance(trades, dict) and trades.get("error"):
            return None
        if not isinstance(trades, list):
            return None

        total_qty = 0.0
        total_cost = 0.0
        for t in trades:
            if float(t['qty']) > 0 and t['isBuyer']:
                trade_price = float(t['price'])
                trade_qty = float(t['qty'])
                total_cost += trade_price * trade_qty
                total_qty += trade_qty

        if total_qty > 0:
            avg_price = total_cost / total_qty
            return avg_price
        else:
            return None

except Exception:
    pass


##############################################
# Bybit
##############################################
try:
    from pybit.unified_trading import HTTP

    bybit_client = HTTP(
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        testnet=False
    )

    def get_symbol_filters(symbol):
        resp = bybit_client.get_instruments_info(category="spot", symbol=symbol)
        if resp['retCode'] != 0 or 'list' not in resp['result']:
            raise Exception(f"symbol info query failed: {resp.get('retMsg', 'Unknown error')}")
        instruments_list = resp['result']['list']
        if not instruments_list:
            raise Exception("No symbol info found.")

        lot_size_filter = instruments_list[0].get('lotSizeFilter', {})
        min_qty = float(lot_size_filter.get('minOrderQty', 0))

        qty_step = lot_size_filter.get('qtyStep', None)
        if qty_step is None:
            base_precision = lot_size_filter.get('basePrecision', '0.01')
            qty_step = float(base_precision)
        else:
            qty_step = float(qty_step)
        return min_qty, qty_step

    def adjust_quantity_to_step(qty, step, min_qty):
        adjusted = math.floor(qty / step) * step
        if adjusted < min_qty:
            return 0.0
        return adjusted

    def get_decimal_places(qty_step):
        if qty_step == 0:
            return 2
        if qty_step >= 1:
            return 0
        return abs(math.floor(math.log10(qty_step)))

    def buy_bybit_coin_usdt_raw(coin, usdt_amount):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            response = bybit_client.get_wallet_balance(accountType="UNIFIED")
            if response['retCode'] != 0:
                return {"error": response['retMsg']}

            usdt_balance = 0.0
            for account_item in response['result']['list']:
                for c in account_item['coin']:
                    if c['coin'].upper() == "USDT":
                        usdt_balance = float(c['walletBalance'])
                        break

            if usdt_balance <= 0:
                return {"error": "No USDT balance"}

            if usdt_amount > usdt_balance:
                return {"error": "Insufficient USDT balance"}

            usdt_to_use = math.floor(usdt_amount * 100) / 100.0
            if usdt_to_use <= 0:
                return {"error": "Too small USDT amount"}

            order_resp = bybit_client.place_order(
                category="spot",
                symbol=symbol,
                side="Buy",
                orderType="MARKET",
                qty=str(usdt_to_use),
                marketUnit="quoteCoin"
            )
            return order_resp
        except Exception as e:
            return {"error": str(e)}

    def sell_all_bybit_coin_raw(coin):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            min_qty, qty_step = get_symbol_filters(symbol)

            response = bybit_client.get_wallet_balance(accountType="UNIFIED")
            if response['retCode'] != 0:
                return {"error": response['retMsg']}

            coin_balance = 0.0
            for account_item in response['result']['list']:
                for c in account_item['coin']:
                    if c['coin'].upper() == coin:
                        coin_balance = float(c['walletBalance'])
                        break

            if coin_balance <= 0:
                return {"error": "No balance to sell"}

            sell_qty = adjust_quantity_to_step(coin_balance, qty_step, min_qty)
            if sell_qty <= 0:
                return {"error": "Quantity too small"}

            decimal_places = get_decimal_places(qty_step)
            qty_str = f"{sell_qty:.{decimal_places}f}"

            order_resp = bybit_client.place_order(
                category="spot",
                symbol=symbol,
                side="Sell",
                orderType="MARKET",
                qty=qty_str
            )
            return order_resp
        except Exception as e:
            return {"error": str(e)}

    def get_recent_bybit_trades_raw(coin):
        try:
            symbol = (coin.upper() + "USDT")
            resp = bybit_client.get_executions(category="spot", symbol=symbol, limit=200)

            if resp['retCode'] != 0:
                return {"error": resp.get('retMsg', 'Unknown error')}

            trades_data = resp.get('result', {}).get('list', [])
            trades_list = []
            for t in trades_data:
                exec_time_str = t.get('execTime', '0')
                exec_time = int(exec_time_str) if exec_time_str.isdigit() else 0

                side_str = t.get('side', '').lower()
                price = t.get('execPrice', '0')
                qty = t.get('execQty', '0')
                sym = t.get('symbol', 'UNKNOWN')
                is_buyer = True if side_str == 'buy' else False

                trades_list.append({
                    'symbol': sym,
                    'price': price,
                    'qty': qty,
                    'time': exec_time,
                    'isBuyer': is_buyer
                })

            trades_list.sort(key=lambda x: x['time'])
            return trades_list
        except Exception as e:
            return {"error": str(e)}

    def calculate_bybit_avg_buy_price(coin):
        trades = get_recent_bybit_trades_raw(coin)
        if isinstance(trades, dict) and trades.get("error"):
            return None
        if not isinstance(trades, list):
            return None

        total_qty = 0.0
        total_cost = 0.0
        for t in trades:
            if t.get('isBuyer'):
                trade_price = float(t['price'])
                trade_qty = float(t['qty'])
                total_cost += trade_price * trade_qty
                total_qty += trade_qty

        if total_qty > 0:
            avg_price = total_cost / total_qty
            return avg_price
        else:
            return None

except Exception as e:
    print(f"Bybit related error: {e}")


##############################################
# Bitget
##############################################
def get_timestamp():
    return str(int(time.time() * 1000))

def sign(method, request_path, timestamp, body_str=""):
    message = timestamp + method + request_path + body_str
    signature = hmac.new(BITGET_SECRET_KEY.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
    signature_b64 = base64.b64encode(signature).decode()
    return signature_b64

def send_request(method, endpoint, params=None, body=None, need_auth=False):
    if params is None:
        params = {}
    if body is None:
        body = {}

    if method.upper() == "GET" and params:
        query_string = urlencode(params)
        request_path = endpoint + "?" + query_string
        url = BITGET_BASE_URL + request_path
        body_str = ""
    else:
        request_path = endpoint
        url = BITGET_BASE_URL + endpoint
        body_str = json.dumps(body) if (body and method.upper() != "GET") else ""

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    if need_auth:
        ts = get_timestamp()
        sig = sign(method.upper(), request_path, ts, body_str)
        headers["ACCESS-KEY"] = BITGET_API_KEY
        headers["ACCESS-SIGN"] = sig
        headers["ACCESS-TIMESTAMP"] = ts
        headers["ACCESS-PASSPHRASE"] = BITGET_PASSPHRASE

    response = requests.request(method, url, headers=headers, data=body_str if method.upper() != "GET" else None)
    try:
        return response.json()
    except:
        return response.text

def check_spot_balance(coin=None, asset_type=None):
    endpoint = "/api/v2/spot/account/assets"
    params = {}
    if coin:
        params["coin"] = coin
    if asset_type:
        params["assetType"] = asset_type
    return send_request("GET", endpoint, params=params, need_auth=True)

def place_spot_order(symbol, side, order_type, force, size, price=None, client_oid=None):
    body = {
        "symbol": symbol,
        "side": side,
        "orderType": order_type,
        "force": force,
        "size": size
    }
    if price and order_type == "limit":
        body["price"] = price
    if client_oid:
        body["clientOid"] = client_oid

    endpoint = "/api/v2/spot/trade/place-order"
    result = send_request("POST", endpoint, body=body, need_auth=True)
    return result

def get_bitget_symbol_info(symbol):
    endpoint = "/api/v2/spot/public/symbols"
    params = {}
    if symbol:
        params["symbol"] = symbol
    resp = send_request("GET", endpoint, params=params, need_auth=False)
    if resp.get("code") != "00000":
        return None

    products = resp.get("data", [])
    for p in products:
        if p.get("symbol") == symbol:
            return p
    return None

def bitget_buy_coin_usdt_raw(coin, usdt_amount):
    coin = coin.upper()
    balance_data = check_spot_balance()
    if balance_data.get("code") != "00000":
        return {"error": balance_data.get('msg', 'Balance query error')}
    available_usdt = "0"
    for b in balance_data.get("data", []):
        if b.get("coin") == "USDT":
            available_usdt = b.get("available", "0")
            break

    usdt_balance = float(available_usdt)
    if usdt_balance <= 0:
        return {"error": "No USDT balance"}
    if usdt_amount > usdt_balance:
        return {"error": "Insufficient USDT balance"}

    usdt_to_use = math.floor(usdt_amount * 100) / 100.0
    if usdt_to_use <= 0:
        return {"error": "Too small USDT amount"}

    symbol = coin + "USDT"
    order_resp = place_spot_order(symbol=symbol, side="buy", order_type="market", force="normal", size=str(usdt_to_use))
    return order_resp

def bitget_sell_all_coin_raw(coin):
    coin = coin.upper()
    balance_data = check_spot_balance()
    if balance_data.get("code") != "00000":
        return {"error": balance_data.get('msg', 'Balance query error')}
    available_amount = "0"
    for b in balance_data.get("data", []):
        if b.get("coin").upper() == coin:
            available_amount = b.get("available", "0")
            break

    amount = float(available_amount)
    if amount <= 0:
        return {"error": "No balance to sell"}

    symbol = coin + "USDT"
    symbol_info = get_bitget_symbol_info(symbol)
    if not symbol_info:
        return {"error": "Symbol info not found"}

    min_trade_amount = float(symbol_info.get("minTradeAmount", "1"))
    quantity_precision = int(symbol_info.get("quantityPrecision", "2"))
    if amount < min_trade_amount:
        return {"error": "Amount too small"}

    max_size = round(amount, quantity_precision)
    step = 10 ** (-quantity_precision)
    safe_size = max_size - step
    if safe_size < min_trade_amount:
        safe_size = min_trade_amount

    size_str = f"{safe_size:.{quantity_precision}f}"
    order_resp = place_spot_order(symbol=symbol, side="sell", order_type="market", force="normal", size=size_str)
    return order_resp

def get_recent_bg_trades_raw(coin):
    try:
        symbol = (coin.upper() + "USDT")
        endpoint = "/api/v2/spot/trade/fills"
        params = {
            "symbol": symbol,
            "limit": "100"
        }

        resp = send_request("GET", endpoint, params=params, need_auth=True)
        if resp.get("code") != "00000":
            return {"error": resp.get("msg", "Unknown error")}

        data = resp.get("data", [])
        trades_list = []
        for t in data:
            side_str = t.get('side', '').lower()
            price = t.get('priceAvg', '0')
            qty = t.get('size', '0')
            sym = t.get('symbol', 'UNKNOWN')
            ctime_str = t.get('cTime', '0')
            exec_time = int(ctime_str) if ctime_str.isdigit() else 0

            is_buyer = True if side_str == 'buy' else False

            trades_list.append({
                'symbol': sym,
                'price': price,
                'qty': qty,
                'time': exec_time,
                'isBuyer': is_buyer
            })

        trades_list.sort(key=lambda x: x['time'])
        return trades_list
    except Exception as e:
        return {"error": str(e)}

def calculate_bg_avg_buy_price(coin):
    trades = get_recent_bg_trades_raw(coin)
    if isinstance(trades, dict) and trades.get("error"):
        return None
    if not isinstance(trades, list):
        return None

    total_qty = 0.0
    total_cost = 0.0
    for t in trades:
        if t.get('isBuyer'):
            trade_price = float(t['price'])
            trade_qty = float(t['qty'])
            total_cost += trade_price * trade_qty
            total_qty += trade_qty

    if total_qty > 0:
        avg_price = total_cost / total_qty
        return avg_price
    else:
        return None

##############################################
# 손익평가
##############################################
def get_current_price(coin):
    symbol = coin.upper() + "USDT"
    ticker = binance_client_cr.get_symbol_ticker(symbol=symbol)
    return float(ticker['price'])

def show_profit_loss_per_account(coin):
    binance_accounts = [
        (binance_client_cr, "CR"),
        (binance_client_lilac, "LILAC"),
        (binance_client_ex, "EX")
    ]

    current_price = get_current_price(coin)

    print("=== Binance PnL ===")
    for client, acc_name in binance_accounts:
        avg_price = calculate_account_avg_buy_price(client, coin)
        if avg_price is not None:
            pnl = current_price - avg_price
            pnl_percent = (pnl / avg_price) * 100.0
            print(f"[BN-{acc_name}] current_price: ${current_price:.3f}, avg_price: ${avg_price:.3f}, pnl: {pnl_percent:.3f}%")
        else:
            print(f"[BN-{acc_name}] no buy history")

    avg_price_bybit = calculate_bybit_avg_buy_price(coin)
    print("=== Bybit PnL ===")
    if avg_price_bybit is not None:
        pnl = current_price - avg_price_bybit
        pnl_percent = (pnl / avg_price_bybit) * 100.0
        print(f"[BB] current_price: ${current_price:.3f}, avg_price: ${avg_price_bybit:.3f}, pnl: {pnl_percent:.3f}%")
    else:
        print("[BB] no buy history")

    avg_price_bg = calculate_bg_avg_buy_price(coin)
    print("=== Bitget PnL ===")
    if avg_price_bg is not None:
        pnl = current_price - avg_price_bg
        pnl_percent = (pnl / avg_price_bg) * 100.0
        print(f"[BG] current_price: ${current_price:.3f}, avg_price: ${avg_price_bg:.3f}, pnl: {pnl_percent:.3f}%")
    else:
        print("[BG] no buy history")



##############################################
# 체결내역 출력 함수
##############################################
def print_trade_history(trades):
    if isinstance(trades, dict) and trades.get("error"):
        print(trades)
        return

    if not trades or len(trades) == 0:
        print("no fills.")
        return

    for t in trades:
        trade_time = datetime.datetime.fromtimestamp(t['time'] / 1000.0)
        side = "bid" if t['isBuyer'] else "ask"
        coin_name = t['symbol'].replace("USDT", "")
        qty = t['qty']
        price = t['price']
        print(f"{trade_time} {side} {qty} {coin_name} at {price}")


##############################################
# 전체 잔고 조회 함수
##############################################
def check_all_balances():
    print("\n=== All Balances ===\n")

    print("=== Binance Spot Balances (3acc) ===")
    get_spot_balance_all()
    print()

    print("=== Bybit Unified Balances ===")
    try:
        response = bybit_client.get_wallet_balance(accountType="UNIFIED")
        if response['retCode'] == 0:
            coins_found = False
            for account_item in response['result']['list']:
                for c in account_item['coin']:
                    wallet_balance = float(c.get('walletBalance', 0))
                    if wallet_balance > 0:
                        print(f"{c['coin']}: balance: {wallet_balance}")
                        coins_found = True
            if not coins_found:
                print("no balance")
        else:
            print(f"balance query failed: {response['retMsg']}")
    except Exception as e:
        print(f"error: {e}")
    print()

    print("=== Bitget Spot Balances ===")
    res = check_spot_balance()
    if res and res.get("code") == "00000":
        data = res.get("data", [])
        if not data:
            print("no balance")
        else:
            coins_found = False
            for b in data:
                coin = b.get("coin")
                available = b.get("available")
                if float(available) > 0:
                    print(f"{coin}: available: {available}")
                    coins_found = True
            if not coins_found:
                print("no balance")
    else:
        print("Bitget balance query failed")

    print("\n=== All balances end ===\n")


##############################################
# 전체매수/전체매도
##############################################
def buy_all(coin, usdt_amount):
    bn_cr_result = buy_binance_coin_usdt_raw(binance_client_cr, coin, usdt_amount)
    bn_lilac_result = buy_binance_coin_usdt_raw(binance_client_lilac, coin, usdt_amount)
    bn_ex_result = buy_binance_coin_usdt_raw(binance_client_ex, coin, usdt_amount)

    bb_result = buy_bybit_coin_usdt_raw(coin, usdt_amount)
    bg_result = bitget_buy_coin_usdt_raw(coin, usdt_amount)

    print("=== buy results ===")
    print("[BN - CR]")
    print(bn_cr_result)
    print("[BN - LILAC]")
    print(bn_lilac_result)
    print("[BN - EX]")
    print(bn_ex_result)
    print("[BB]")
    print(bb_result)
    print("[BG]")
    print(bg_result)

def sell_all(coin):
    bn_cr_result = sell_all_binance_coin_raw(binance_client_cr, coin)
    bn_lilac_result = sell_all_binance_coin_raw(binance_client_lilac, coin)
    bn_ex_result = sell_all_binance_coin_raw(binance_client_ex, coin)

    bb_result = sell_all_bybit_coin_raw(coin)
    bg_result = bitget_sell_all_coin_raw(coin)

    print("=== sell results ===")
    print("[BN - CR]")
    print(bn_cr_result)
    print("[BN - LILAC]")
    print(bn_lilac_result)
    print("[BN - EX]")
    print(bn_ex_result)
    print("[BB]")
    print(bb_result)
    print("[BG]")
    print(bg_result)


##############################################
# 메인 로직
##############################################
test_run = run_tests

while True:
    print("\nSelect action:")
    print("1.test 2.BN 3.BB buy 4.BB sell 5.BG buy 6.BG sell 7.exit")
    print("Or use 'buy.COIN.value', 'sell.COIN', 'show_trx.COIN', 'show_pnl.COIN', 'show_bal' commands")
    cmd = input("Input: ").strip()

    # 1. Test notices
    if cmd == '1':
        if test_run:
            test_run()
        continue

    # 2. Binance account operations
    elif cmd == '2':
        print("Select BN account: 1(CR), 2(LILAC), 3(EX)")
        bn_acc_choice = input("Input: ").strip().lower()
        if bn_acc_choice in ['1', 'cr']:
            bn_client = (binance_client_cr, "CR")
        elif bn_acc_choice in ['2', 'lilac']:
            bn_client = (binance_client_lilac, "LILAC")
        elif bn_acc_choice in ['3', 'ex']:
            bn_client = (binance_client_ex, "EX")
        else:
            print("invalid BN account")
            continue

        print(f"Selected BN[{bn_client[1]}]. 1(buy), 2(sell)")
        action_choice = input("Input: ").strip()

        if action_choice == '1':
            get_spot_balance_for_client(bn_client[0], bn_client[1])
            coin_input = input(f"{bn_client[1]} buy coin: ").strip()
            if not coin_input:
                print("no coin, cancel")
                continue
            try:
                usdt_str = input(f"{coin_input.upper()} buy USDT amount: ").strip()
                usdt_amount = float(usdt_str)
            except:
                print("invalid amount, cancel")
                continue
            order_data = buy_binance_coin_usdt_raw(bn_client[0], coin_input, usdt_amount)
            print(order_data)

        elif action_choice == '2':
            get_spot_balance_for_client(bn_client[0], bn_client[1])
            coin_input = input(f"{bn_client[1]} sell all coin: ").strip()
            if coin_input:
                order_data = sell_all_binance_coin_raw(bn_client[0], coin_input)
                print(order_data)
            else:
                print("no coin, cancel")
        else:
            print("invalid selection, cancel")
        continue

    # 3. BB Buy
    elif cmd == '3':
        coin_input = input("BB (buy) coin: ").strip()
        if not coin_input:
            print("no coin, cancel")
            continue
        try:
            usdt_str = input(f"{coin_input.upper()} buy USDT amount: ").strip()
            usdt_amount = float(usdt_str)
        except:
            print("invalid amount, cancel")
            continue
        order_data = buy_bybit_coin_usdt_raw(coin_input, usdt_amount)
        print(order_data)
        continue

    # 4. BB Sell
    elif cmd == '4':
        coin_input = input("BB (sell) all coin: ").strip()
        if coin_input:
            order_data = sell_all_bybit_coin_raw(coin_input)
            print(order_data)
        else:
            print("no coin, cancel")
        continue

    # 5. BG Buy
    elif cmd == '5':
        coin_input = input("BG (buy) coin: ").strip()
        if not coin_input:
            print("no coin, cancel")
            continue
        try:
            usdt_str = input(f"{coin_input.upper()} buy USDT amount: ").strip()
            usdt_amount = float(usdt_str)
        except:
            print("invalid amount, cancel")
            continue
        order_data = bitget_buy_coin_usdt_raw(coin_input, usdt_amount)
        print(order_data)
        continue

    # 6. BG Sell
    elif cmd == '6':
        coin_input = input("BG (sell) all coin: ").strip()
        if coin_input:
            order_data = bitget_sell_all_coin_raw(coin_input)
            print(order_data)
        else:
            print("no coin, cancel")
        continue

    # 7. Exit
    elif cmd == '7':
        print("Exiting program.")
        break

    # Advanced commands
    elif cmd.startswith("buy."):
        parts = cmd.split(".")
        if len(parts) == 3:
            c = parts[1]
            value_str = parts[2]
            try:
                value = float(value_str)
                buy_all(c, value)
            except:
                print("invalid value")
        else:
            print("format: buy.COIN.value")
        continue

    elif cmd.startswith("sell."):
        parts = cmd.split(".")
        if len(parts) == 2:
            c = parts[1]
            sell_all(c)
        else:
            print("format: sell.COIN")
        continue

    elif cmd.startswith("show_trx."):
        parts = cmd.split(".")
        if len(parts) == 2:
            c = parts[1].upper()  # COIN 이름 대문자로 변환
            print(f"=== Transaction History for {c} ===")
            try:
                # Binance 거래 내역
                print(f"=== Binance (CR) [{c}] ===")
                cr_trades = get_recent_trades_raw(binance_client_cr, c)
                print_trade_history(cr_trades)

                print(f"=== Binance (LILAC) [{c}] ===")
                lilac_trades = get_recent_trades_raw(binance_client_lilac, c)
                print_trade_history(lilac_trades)

                print(f"=== Binance (EX) [{c}] ===")
                ex_trades = get_recent_trades_raw(binance_client_ex, c)
                print_trade_history(ex_trades)

                # Bybit 거래 내역
                print(f"=== Bybit [{c}] ===")
                bb_trades = get_recent_bybit_trades_raw(c)
                print_trade_history(bb_trades)

                # Bitget 거래 내역
                print(f"=== Bitget [{c}] ===")
                bg_trades = get_recent_bg_trades_raw(c)
                print_trade_history(bg_trades)

            except Exception as e:
                print(f"Error fetching transactions for {c}: {e}")
        else:
            print("format: show_trx.COIN")
        continue

    elif cmd.startswith("show_pnl."):
        parts = cmd.split(".")
        if len(parts) == 2:
            c = parts[1]
            show_profit_loss_per_account(c)
        else:
            print("format: show_pnl.COIN")
        continue

    elif cmd == "show_bal":
        print("\n=== All Wallet Balances ===\n")

        print("=== Binance Spot Balances (3acc) ===")
        get_spot_balance_all()
        print()

        print("=== Bybit Unified Balances (1acc) ===")
        try:
            response = bybit_client.get_wallet_balance(accountType="UNIFIED")
            if response['retCode'] == 0:
                for account_item in response['result']['list']:
                    for c in account_item['coin']:
                        wallet_balance = float(c.get('walletBalance', 0))
                        if wallet_balance > 0:
                            print(f"{c['coin']}: balance: {wallet_balance}")
            else:
                print(f"Bybit balance query failed: {response['retMsg']}")
        except Exception as e:
            print(f"Bybit error: {e}")
        print()

        print("=== Bitget Spot Balances (1acc) ===")
        try:
            res = check_spot_balance()
            if res and res.get("code") == "00000":
                data = res.get("data", [])
                if not data:
                    print("no balance")
                else:
                    for b in data:
                        coin = b.get("coin")
                        available = float(b.get("available", 0))
                        if available > 0:
                            print(f"{coin}: available: {available}")
            else:
                print("Bitget balance query failed")
        except Exception as e:
            print(f"Bitget error: {e}")
        print("\n=== Wallet balances end ===\n")


    # Default invalid command handler
    else:
        print("No such feature.")
