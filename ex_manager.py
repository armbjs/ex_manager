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
    import datetime

    binance_client_cr = Client(BINANCE_API_KEY_CR, BINANCE_API_SECRET_CR)
    binance_client_lilac = Client(BINANCE_API_KEY_LILAC, BINANCE_API_SECRET_LILAC)
    binance_client_ex = Client(BINANCE_API_KEY_EX, BINANCE_API_SECRET_EX)

    def get_spot_balance_for_client(client, account_name):
        print(f"\n=== Binance 스팟 지갑 잔고 ({account_name}) ===")
        try:
            account_info = client.get_account()
            balances = [
                asset for asset in account_info['balances']
                if float(asset['free']) > 0 or float(asset['locked']) > 0
            ]
            if not balances:
                print("잔고가 없습니다.")
            for balance_item in balances:
                coin_name = balance_item['asset']
                free_amount = float(balance_item['free'])
                locked_amount = float(balance_item['locked'])
                print(f"{coin_name}: 사용 가능: {free_amount}, 잠금: {locked_amount}")
        except Exception as e:
            print(f"에러 발생 ({account_name}): {e}")

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

    # 추가: 체결내역 조회 함수
    def get_recent_trades_raw(client, coin):
        try:
            symbol = (coin.upper() + "USDT")
            trades = client.get_my_trades(symbol=symbol, limit=200)
            return trades
        except Exception as e:
            return {"error": str(e)}

    def print_trade_history(trades):
        # trades는 get_recent_trades_raw 로 가져온 리스트 형태의 체결내역
        # 각 항목 예: {'symbol': 'AVAUSDT', 'id': 38642971, 'orderId': ..., 'price': '3.09400000', 'qty': '27.50000000', ... 'time': 1720076234988, 'isBuyer': True/False, ...}
        
        if isinstance(trades, dict) and trades.get("error"):
            # 에러 발생 시 에러 메세지 출력
            print(trades)
            return

        if not trades or len(trades) == 0:
            print("체결내역이 없습니다.")
            return

        for t in trades:
            # 시간 변환
            trade_time = datetime.datetime.fromtimestamp(t['time'] / 1000.0)
            side = "bid" if t['isBuyer'] else "ask"
            coin_name = t['symbol'].replace("USDT", "")  # 'AVAUSDT' -> 'AVA'
            qty = t['qty']
            price = t['price']

            # 원하는 형식으로 출력
            print(f"{trade_time} {side} {qty} {coin_name} at {price}")

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
            raise Exception(f"심볼 정보 조회 실패: {resp.get('retMsg', 'Unknown error')}")
        instruments_list = resp['result']['list']
        if not instruments_list:
            raise Exception("해당 심볼 정보를 찾을 수 없습니다.")

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

except Exception as e:
    print(f"Bybit 관련 코드에서 에러 발생: {e}")


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


##############################################
# 전체 잔고 조회 함수
##############################################
def check_all_balances():
    print("\n=== 전체 잔고 조회를 시작합니다. ===\n")

    print("=== Binance Spot 잔고 조회 (3계정) ===")
    get_spot_balance_all()
    print()

    print("=== Bybit Unified 잔고 조회 ===")
    try:
        response = bybit_client.get_wallet_balance(accountType="UNIFIED")
        if response['retCode'] == 0:
            coins_found = False
            for account_item in response['result']['list']:
                for c in account_item['coin']:
                    wallet_balance = float(c.get('walletBalance', 0))
                    if wallet_balance > 0:
                        print(f"{c['coin']}: 잔고: {wallet_balance}")
                        coins_found = True
            if not coins_found:
                print("잔고가 없습니다.")
        else:
            print(f"잔고 조회 실패: {response['retMsg']}")
    except Exception as e:
        print(f"에러 발생: {e}")
    print()

    print("=== Bitget Spot 잔고 조회 ===")
    res = check_spot_balance()
    if res and res.get("code") == "00000":
        data = res.get("data", [])
        if not data:
            print("잔고가 없습니다.")
        else:
            coins_found = False
            for b in data:
                coin = b.get("coin")
                available = b.get("available")
                if float(available) > 0:
                    print(f"{coin}: 사용 가능: {available}")
                    coins_found = True
            if not coins_found:
                print("잔고가 없습니다.")
    else:
        print("Bitget 잔고 조회 실패")

    print("\n=== 전체 잔고 조회를 마쳤습니다. ===\n")


##############################################
# 전체매수/전체매도
##############################################
def buy_all(coin, usdt_amount):
    bn_cr_result = buy_binance_coin_usdt_raw(binance_client_cr, coin, usdt_amount)
    bn_lilac_result = buy_binance_coin_usdt_raw(binance_client_lilac, coin, usdt_amount)
    bn_ex_result = buy_binance_coin_usdt_raw(binance_client_ex, coin, usdt_amount)

    bb_result = buy_bybit_coin_usdt_raw(coin, usdt_amount)
    bg_result = bitget_buy_coin_usdt_raw(coin, usdt_amount)

    print("=== 전체매수 결과 ===")
    print("[BN - CR 계정]")
    print(bn_cr_result)
    print("[BN - LILAC 계정]")
    print(bn_lilac_result)
    print("[BN - EX 계정]")
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

    print("=== 전체매도 결과 ===")
    print("[BN - CR 계정]")
    print(bn_cr_result)
    print("[BN - LILAC 계정]")
    print(bn_lilac_result)
    print("[BN - EX 계정]")
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
    print("\n프로그램 실행 시 동작할 기능을 입력하시오")
    print("1.테스트 2.지갑조회 3.BN 4.BB매수 5.BB매도 6.BG매수 7.BG매도 8.종료")
    print("또는 '전체매수.XRP.20', '전체매도.XRP', '체결조회.XRP' 형태로 명령 가능")
    choice = input("입력: ").strip()

    if choice.startswith("전체매수."):
        parts = choice.split(".")
        if len(parts) == 3:
            c = parts[1]
            usdt_str = parts[2]
            try:
                ua = float(usdt_str)
                buy_all(c, ua)
            except:
                print("수량이 유효하지 않습니다.")
        else:
            print("전체매수 명령 형식: 전체매수.COIN.USDT_AMOUNT")
        continue

    if choice.startswith("전체매도."):
        parts = choice.split(".")
        if len(parts) == 2:
            c = parts[1]
            sell_all(c)
        else:
            print("전체매도 명령 형식: 전체매도.COIN")
        continue

    # 체결조회 명령 처리
    if choice.startswith("체결조회."):
        parts = choice.split(".")
        if len(parts) == 2:
            c = parts[1]
            # BN 3계정 모두 체결내역 조회 및 깔끔한 출력
            print(f"=== Binance 체결내역 조회 (CR 계정) [{c.upper()}] ===")
            cr_trades = get_recent_trades_raw(binance_client_cr, c)
            print_trade_history(cr_trades)

            print(f"=== Binance 체결내역 조회 (LILAC 계정) [{c.upper()}] ===")
            lilac_trades = get_recent_trades_raw(binance_client_lilac, c)
            print_trade_history(lilac_trades)

            print(f"=== Binance 체결내역 조회 (EX 계정) [{c.upper()}] ===")
            ex_trades = get_recent_trades_raw(binance_client_ex, c)
            print_trade_history(ex_trades)
        else:
            print("체결조회 명령 형식: 체결조회.COIN")
        continue

    if choice == '1':
        if test_run:
            test_run()

    elif choice == '2':
        check_all_balances()

    elif choice == '3':
        print("BN 계정을 선택해주세요: 1(CR), 2(LILAC), 3(EX)")
        bn_acc_choice = input("입력: ").strip().lower()
        if bn_acc_choice in ['1', 'cr']:
            bn_client = (binance_client_cr, "CR")
        elif bn_acc_choice in ['2', 'lilac']:
            bn_client = (binance_client_lilac, "LILAC")
        elif bn_acc_choice in ['3', 'ex']:
            bn_client = (binance_client_ex, "EX")
        else:
            print("유효한 BN 계정을 입력하지 않아 거래를 취소합니다.")
            continue

        print(f"BN거래소[{bn_client[1]} 계정]를 선택했습니다. 1(매수), 2(매도)")
        action_choice = input("입력: ").strip()

        if action_choice == '1':
            get_spot_balance_for_client(bn_client[0], bn_client[1])
            coin_input = input(f"{bn_client[1]} 계정 매수할 코인명: ").strip()
            if not coin_input:
                print("코인명을 입력하지 않아 매수를 취소합니다.")
                continue
            try:
                usdt_str = input(f"{coin_input.upper()} 매수할 USDT 수량: ").strip()
                usdt_amount = float(usdt_str)
            except:
                print("유효한 수량을 입력하지 않아 매수를 취소합니다.")
                continue
            order_data = buy_binance_coin_usdt_raw(bn_client[0], coin_input, usdt_amount)
            print(order_data)

        elif action_choice == '2':
            get_spot_balance_for_client(bn_client[0], bn_client[1])
            coin_input = input(f"{bn_client[1]} 계정 전액 매도할 코인명: ").strip()
            if coin_input:
                order_data = sell_all_binance_coin_raw(bn_client[0], coin_input)
                print(order_data)
            else:
                print("코인명을 입력하지 않아 매도를 취소합니다.")
        else:
            print("유효한 선택을 하지 않아 거래를 취소합니다.")

    elif choice == '4':
        # BB 매수
        resp = bybit_client.get_wallet_balance(accountType="UNIFIED")
        if resp['retCode'] == 0:
            coins_found = False
            for account_item in resp['result']['list']:
                for c in account_item['coin']:
                    wbal = float(c.get('walletBalance', 0))
                    if wbal > 0:
                        print(f"{c['coin']}: 잔고: {wbal}")
                        coins_found = True
            if not coins_found:
                print("잔고가 없습니다.")
        else:
            print(f"잔고 조회 실패: {resp['retMsg']}")

        coin_input = input("BB거래소(매수) 매수할 코인명: ").strip()
        if not coin_input:
            print("코인명을 입력하지 않아 매수를 취소합니다.")
            continue

        try:
            usdt_str = input(f"{coin_input.upper()} 매수할 USDT 수량: ").strip()
            usdt_amount = float(usdt_str)
        except:
            print("유효한 수량을 입력하지 않아 매수를 취소합니다.")
            continue

        order_data = buy_bybit_coin_usdt_raw(coin_input, usdt_amount)
        print(order_data)

    elif choice == '5':
        # BB 매도
        resp = bybit_client.get_wallet_balance(accountType="UNIFIED")
        if resp['retCode'] == 0:
            coins_found = False
            for account_item in resp['result']['list']:
                for c in account_item['coin']:
                    wbal = float(c.get('walletBalance', 0))
                    if wbal > 0:
                        print(f"{c['coin']}: 잔고: {wbal}")
                        coins_found = True
            if not coins_found:
                print("잔고가 없습니다.")
        else:
            print(f"잔고 조회 실패: {resp['retMsg']}")

        coin_input = input("BB거래소(매도) 전액 매도할 코인명: ").strip()
        if coin_input:
            order_data = sell_all_bybit_coin_raw(coin_input)
            print(order_data)
        else:
            print("코인명을 입력하지 않아 매도를 취소합니다.")

    elif choice == '6':
        # BG 매수
        res = check_spot_balance()
        if res and res.get("code") == "00000":
            data = res.get("data", [])
            print("\n=== Bitget Spot 지갑 잔고 ===")
            if not data:
                print("잔고가 없습니다.")
            else:
                coins_found = False
                for b in data:
                    c_name = b.get("coin")
                    av = b.get("available")
                    if float(av) > 0:
                        print(f"{c_name}: 사용 가능: {av}")
                        coins_found = True
                if not coins_found:
                    print("잔고가 없습니다.")
        else:
            print("Bitget 잔고 조회 실패")

        coin_input = input("BG거래소(매수) 매수할 코인명: ").strip()
        if not coin_input:
            print("코인명을 입력하지 않아 매수를 취소합니다.")
            continue

        try:
            usdt_str = input(f"{coin_input.upper()} 매수할 USDT 수량: ").strip()
            usdt_amount = float(usdt_str)
        except:
            print("유효한 수량을 입력하지 않아 매수를 취소합니다.")
            continue

        order_data = bitget_buy_coin_usdt_raw(coin_input, usdt_amount)
        print(order_data)

    elif choice == '7':
        # BG 매도
        res = check_spot_balance()
        if res and res.get("code") == "00000":
            data = res.get("data", [])
            print("\n=== Bitget Spot 지갑 잔고 ===")
            if not data:
                print("잔고가 없습니다.")
            else:
                coins_found = False
                for b in data:
                    c_name = b.get("coin")
                    av = b.get("available")
                    if float(av) > 0:
                        print(f"{c_name}: 사용 가능: {av}")
                        coins_found = True
                if not coins_found:
                    print("잔고가 없습니다.")
        else:
            print("Bitget 잔고 조회 실패")

        coin_input = input("BG거래소(매도) 전액 매도할 코인명: ").strip()
        if coin_input:
            order_data = bitget_sell_all_coin_raw(coin_input)
            print(order_data)
        else:
            print("코인명을 입력하지 않아 매도를 취소합니다.")

    elif choice == '8':
        print("프로그램을 종료합니다.")
        break
    else:
        print("해당 기능을 찾을 수 없습니다. (1=테스트/2=지갑조회/3=BN/4=BB매수/5=BB매도/6=BG매수/7=BG매도/8=종료)")
