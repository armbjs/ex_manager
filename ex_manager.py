#############################################
# 수정된 메인 프로그램 코드
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

    # Redis 설정
    redis_server_addr = 'vultr-prod-3a00cfaa-7dc1-4fed-af5b-19dd1a19abf2-vultr-prod-cd4a.vultrdb.com'
    redis_server_port = '16752'
    redis_server_db_index = '0'
    redis_server_username = 'armbjs'
    redis_server_password = 'xkqfhf12'
    redis_server_ssl = 'True'

    redis_publish_channel_name_prefix = 'TEST_NEW_NOTICES'
    redis_publish_binance_channel = 'BINANCE_NEW_NOTICES'
    redis_stream_key_buy = 'fake_pubsub_massage:purchased_coins'

    # Redis 클라이언트 생성
    redis_client = redis.Redis(
        host=redis_server_addr,
        port=int(redis_server_port),
        db=int(redis_server_db_index),
        username=redis_server_username,
        password=redis_server_password,
        ssl=redis_server_ssl,
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

        upbit_notice_en2 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"Market Support for Tasdas({coin_symbol}), Ripple Network(XRP) (BTC, USDT Market)",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": "Trade",
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }

        upbit_notice_kr1 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"[거래] {coin_symbol}(TestCoin), XRP(리플) 신규 거래지원 안내 (BTC, USDT 마켓)",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": None,
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }

        upbit_notice_kr2 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"렌더토큰({coin_symbol}) KRW, USDT 마켓 디지털 자산 추가",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": None,
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }

        bithumb_notice = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"리플(XRP) 원화 마켓 추가 이벤트 에어드랍",
            "content": None,
            "exchange": "BITHUMB",
            "url": "https://feed.bithumb.com/notice/1645302",
            "category": None,
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }

        try:
            # UPBIT 공지 발송
            upbit_notice_en1_json = json.dumps(upbit_notice_en1, ensure_ascii=False)
            redis_client.publish(redis_publish_channel_name_prefix, upbit_notice_en1_json)
            logger.info(f"Published UPBIT test notice for {coin_symbol}")
            logger.info(f"UPBIT notice data: {upbit_notice_en1_json}")

            time.sleep(2)

            upbit_notice_en2_json = json.dumps(upbit_notice_en2, ensure_ascii=False)
            redis_client.publish(redis_publish_channel_name_prefix, upbit_notice_en2_json)
            logger.info(f"Published UPBIT test notice for {coin_symbol}")
            logger.info(f"UPBIT notice data: {upbit_notice_en2_json}")

            time.sleep(2)

            upbit_notice_kr1_json = json.dumps(upbit_notice_kr1, ensure_ascii=False)
            redis_client.publish(redis_publish_channel_name_prefix, upbit_notice_kr1_json)
            logger.info(f"Published BITHUMB test notice for {coin_symbol}")
            logger.info(f"UPBIT notice data: {upbit_notice_kr1_json}")

            time.sleep(2)

            upbit_notice_kr2_json = json.dumps(upbit_notice_kr2, ensure_ascii=False)
            redis_client.publish(redis_publish_channel_name_prefix, upbit_notice_kr2_json)
            logger.info(f"Published UPBIT digital asset addition notice for {coin_symbol}")
            logger.info(f"UPBIT notice data: {upbit_notice_kr2_json}")

            time.sleep(2)

            bithumb_notice_json = json.dumps(bithumb_notice, ensure_ascii=False)
            redis_client.publish(redis_publish_channel_name_prefix, bithumb_notice_json)
            logger.info(f"Published UPBIT digital asset addition notice for {coin_symbol}")
            logger.info(f"UPBIT notice data: {bithumb_notice_json}")

        except Exception as e:
            logger.error(f"Error publishing notices: {e}")

    def run_tests():
        logger.info("Executing test notices")
        publish_test_notices()

except Exception:
    pass

##############################################
# BINANCE 계정 3개 상수 정의 (CR, LILAC, EX)
##############################################
BINANCE_API_KEY_CR = "CUvOhge0SRuYgon2edHchbOVIX3d70GQCevrvRMPNsL5LJ1mTdtVKFfB3FDu9hay"
BINANCE_API_SECRET_CR = "rhHey3FnPHsoW1lhWgYmfdZmJJVkqTxL1kjBa6Y4TrBgV0AyNk2EIPqRaR8rNykf"

BINANCE_API_KEY_LILAC = "nn0r9JF9CjJ2vd1lsCbyqmmcmK5HWGX1jqL0ukuoZIJhDwRLy0Q1VOd2mLcee9Ur"
BINANCE_API_SECRET_LILAC = "8UkN9v0ZIhAaVd23lcp1IKbyj01WFCxSDkMR88lhRMp05I5qySVyoO9xDMUfkJuH"

BINANCE_API_KEY_EX = "F3UZITsSHTg45JaeDx50eaONy1eQixdHWETOOHQV9w19YzUMWebgJCTO0qtEH24Y"
BINANCE_API_SECRET_EX = "jqB0T9AAQbd4IMbATXGBqFGCIdiQHiO5G0e4WcOSNaUbBwDPM6dtiDirHa3qbfQf"


##############################################
# 2번 파트 (Binance: BN 매도/매수/잔고 조회)
##############################################
try:
    from binance.client import Client
    from binance.enums import *

    # 3개 계정용 Client
    binance_client_cr = Client(BINANCE_API_KEY_CR, BINANCE_API_SECRET_CR)
    binance_client_lilac = Client(BINANCE_API_KEY_LILAC, BINANCE_API_SECRET_LILAC)
    binance_client_ex = Client(BINANCE_API_KEY_EX, BINANCE_API_SECRET_EX)

    def get_spot_balance_for_client(client, account_name):
        try:
            account = client.get_account()
            balances = [
                asset for asset in account['balances']
                if float(asset['free']) > 0 or float(asset['locked']) > 0
            ]
            print(f"\n=== Binance 스팟 지갑 잔고 ({account_name}) ===")
            if not balances:
                print("잔고가 없습니다.")
            for balance in balances:
                coin = balance['asset']
                free = float(balance['free'])
                locked = float(balance['locked'])
                print(f"{coin}: 사용 가능: {free}, 잠금: {locked}")
        except Exception as e:
            print(f"에러 발생 ({account_name}): {e}")

    def get_spot_balance_all():
        # CR, LILAC, EX 모두 조회
        get_spot_balance_for_client(binance_client_cr, "CR")
        get_spot_balance_for_client(binance_client_lilac, "LILAC")
        get_spot_balance_for_client(binance_client_ex, "EX")

    def buy_binance_coin_with_usdt_for_client(client, account_name, coin, usdt_amount):
        try:
            coin = coin.upper()
            usdt_info = client.get_asset_balance(asset='USDT')
            if not usdt_info:
                print(f"{account_name} USDT 잔고 조회 실패")
                return
            usdt_balance = float(usdt_info['free'])
            if usdt_balance <= 0:
                print(f"{account_name} USDT 잔고가 없습니다.")
                return

            if usdt_amount > usdt_balance:
                print(f"{account_name} USDT 잔고가 부족합니다.")
                return

            symbol = coin + "USDT"
            ticker = client.get_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])

            usdt_to_use = math.floor(usdt_amount * 100) / 100.0
            if usdt_to_use <= 0:
                print(f"{account_name} USDT 금액이 너무 적어서 주문할 수 없습니다.")
                return

            quantity = usdt_to_use / price
            quantity = float(int(quantity))

            if quantity <= 0:
                print(f"{account_name} USDT 금액이 너무 적어서 주문할 수 없습니다.")
                return

            order = client.create_order(
                symbol=symbol,
                side=SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )
            print(f"{account_name} BN매수 완료! 약 {usdt_to_use} USDT 상당의 {coin} 매수")
            get_spot_balance_for_client(client, account_name)
        except Exception as e:
            print(f"에러 발생 ({account_name}): {e}")

    def sell_all_binance_coin_for_client(client, account_name, coin):
        try:
            coin = coin.upper()
            balance_info = client.get_asset_balance(asset=coin)
            if not balance_info:
                print(f"{account_name} {coin} 잔고 조회 실패")
                return
            balance = float(balance_info['free'])

            if balance <= 0:
                print(f"{account_name} 판매할 {coin}가 없습니다.")
                return

            quantity = float(int(balance))

            symbol = coin + "USDT"
            order = client.create_order(
                symbol=symbol,
                side=SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )

            print(f"{account_name} BN매도 완료! 수량: {quantity} {coin}")
            get_spot_balance_for_client(client, account_name)
        except Exception as e:
            print(f"에러 발생 ({account_name}): {e}")

except Exception:
    pass

##############################################
# 3번 파트 (Bybit: BB 매도/매수/잔고 조회)
##############################################
try:
    from pybit.unified_trading import HTTP

    BYBIT_API_KEY = 'FzG8fr6fGbK5JFPhrq'
    BYBIT_API_SECRET = '3Ign1vEI1Qj8Kx0B8ABBpjEnddicWEXQGRI3'

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

        lotSizeFilter = instruments_list[0].get('lotSizeFilter', {})
        min_qty = float(lotSizeFilter.get('minOrderQty', 0))

        qty_step = lotSizeFilter.get('qtyStep', None)
        if qty_step is None:
            base_precision = lotSizeFilter.get('basePrecision', '0.01')
            qty_step = float(base_precision)
        else:
            qty_step = float(qty_step)

        return min_qty, qty_step

    def adjust_qty_to_step(qty, step, min_qty):
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

    def get_bybit_balance():
        try:
            response = bybit_client.get_wallet_balance(accountType="UNIFIED")
            if response['retCode'] == 0:
                print("\n=== Bybit Unified 계정 잔고 ===")
                coins_found = False
                for account in response['result']['list']:
                    for c in account['coin']:
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

    def sell_all_bybit_coin(coin):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            min_qty, qty_step = get_symbol_filters(symbol)

            response = bybit_client.get_wallet_balance(accountType="UNIFIED")
            if response['retCode'] != 0:
                print(f"잔고 조회 실패: {response['retMsg']}")
                return

            coin_balance = 0.0
            for account in response['result']['list']:
                for c in account['coin']:
                    if c['coin'].upper() == coin:
                        coin_balance = float(c['walletBalance'])
                        break

            if coin_balance <= 0:
                print(f"판매할 {coin}가 없습니다.")
                return

            sell_qty = adjust_qty_to_step(coin_balance, qty_step, min_qty)
            if sell_qty <= 0:
                print("판매 가능한 최소 단위에 미치지 못합니다.")
                return

            decimal_places = get_decimal_places(qty_step)
            qty_str = f"{sell_qty:.{decimal_places}f}"

            order_resp = bybit_client.place_order(
                category="spot",
                symbol=symbol,
                side="Sell",
                orderType="MARKET",
                qty=qty_str
            )

            if order_resp['retCode'] == 0:
                print(f"BB매도 완료! 수량: {qty_str} {coin}")
            else:
                print(f"매도 실패: {order_resp['retMsg']}")
        except Exception as e:
            print(f"에러 발생: {e}")

    def buy_bybit_coin_with_usdt(coin, usdt_amount):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            response = bybit_client.get_wallet_balance(accountType="UNIFIED")
            if response['retCode'] != 0:
                print(f"잔고 조회 실패: {response['retMsg']}")
                return

            usdt_balance = 0.0
            for account in response['result']['list']:
                for c in account['coin']:
                    if c['coin'].upper() == "USDT":
                        usdt_balance = float(c['walletBalance'])
                        break

            if usdt_balance <= 0:
                print("USDT 잔고가 없습니다.")
                return

            if usdt_amount > usdt_balance:
                print("USDT 잔고가 부족합니다.")
                return

            usdt_to_use = math.floor(usdt_amount * 100) / 100.0
            if usdt_to_use <= 0:
                print("USDT 금액이 너무 적어서 주문할 수 없습니다.")
                return

            order_resp = bybit_client.place_order(
                category="spot",
                symbol=symbol,
                side="Buy",
                orderType="MARKET",
                qty=str(usdt_to_use),
                marketUnit="quoteCoin"
            )

            if order_resp['retCode'] == 0:
                print(f"BB매수 완료! 약 {usdt_to_use} USDT 상당의 {coin} 매수")
            else:
                print(f"매수 실패: {order_resp['retMsg']}")
        except Exception as e:
            print(f"에러 발생: {e}")

except Exception as e:
    print(f"Bybit 관련 코드에서 에러 발생: {e}")

##############################################
# 4번 파트 (Bitget: BG 매도/매수/잔고 조회)
##############################################
BG_API_KEY = "bg_7126c08570667fadf280eb381c4107c8"
BG_SECRET_KEY = "9a251d56cfc9ba75bb622b25d5b695baa5ac438fb2c4400711af296450a51706"
BG_PASSPHRASE = "wlthrwjrdmfh"
BASE_URL = "https://api.bitget.com"

def get_timestamp():
    return str(int(time.time() * 1000))

def sign(method, request_path, timestamp, body_str=""):
    message = timestamp + method + request_path + body_str
    signature = hmac.new(BG_SECRET_KEY.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
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
        url = BASE_URL + request_path
        body_str = ""
    else:
        request_path = endpoint
        url = BASE_URL + endpoint
        body_str = json.dumps(body) if (body and method.upper() != "GET") else ""

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    if need_auth:
        ts = get_timestamp()
        sig = sign(method.upper(), request_path, ts, body_str)
        headers["ACCESS-KEY"] = BG_API_KEY
        headers["ACCESS-SIGN"] = sig
        headers["ACCESS-TIMESTAMP"] = ts
        headers["ACCESS-PASSPHRASE"] = BG_PASSPHRASE

    response = requests.request(method, url, headers=headers, data=body_str if method.upper() != "GET" else None)
    try:
        return response.json()
    except:
        return response.text

def check_spot_balance(coin=None, assetType=None):
    endpoint = "/api/v2/spot/account/assets"
    params = {}
    if coin:
        params["coin"] = coin
    if assetType:
        params["assetType"] = assetType
    return send_request("GET", endpoint, params=params, need_auth=True)

def place_spot_order(symbol, side, orderType, force, size, price=None, clientOid=None):
    body = {
        "symbol": symbol,
        "side": side,
        "orderType": orderType,
        "force": force,
        "size": size
    }
    if price and orderType == "limit":
        body["price"] = price
    if clientOid:
        body["clientOid"] = clientOid

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
        print("심볼 정보 조회 실패:", resp)
        return None

    products = resp.get("data", [])
    for p in products:
        if p.get("symbol") == symbol:
            return p
    return None

def print_bitget_balances():
    res = check_spot_balance()
    if res and res.get("code") == "00000":
        print("\n=== Bitget Spot 지갑 잔고 ===")
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

def bitget_buy_coin_with_usdt(coin, usdt_amount):
    coin = coin.upper()
    balance_data = check_spot_balance()
    if balance_data.get("code") != "00000":
        print("잔고 조회 오류:", balance_data)
        return
    available_usdt = "0"
    for b in balance_data.get("data", []):
        if b.get("coin") == "USDT":
            available_usdt = b.get("available", "0")
            break

    usdt_balance = float(available_usdt)
    if usdt_balance <= 0:
        print("USDT 잔고가 부족합니다.")
        return

    if usdt_amount > usdt_balance:
        print("USDT 잔고가 부족합니다.")
        return

    usdt_to_use = math.floor(usdt_amount * 100) / 100.0
    if usdt_to_use <= 0:
        print("USDT 금액이 너무 적어서 주문할 수 없습니다.")
        return

    symbol = coin + "USDT"
    order_resp = place_spot_order(symbol=symbol, side="buy", orderType="market", force="normal", size=str(usdt_to_use))
    if order_resp.get("code") == "00000":
        print(f"BG매수 완료! 약 {usdt_to_use} USDT 상당의 {coin} 매수")
    else:
        print(f"매수 실패: {order_resp.get('msg')}")

def bitget_sell_all_coin(coin):
    coin = coin.upper()
    balance_data = check_spot_balance()
    if balance_data.get("code") != "00000":
        print("잔고 조회 오류:", balance_data)
        return
    available_amount = "0"
    for b in balance_data.get("data", []):
        if b.get("coin").upper() == coin:
            available_amount = b.get("available", "0")
            break

    amount = float(available_amount)
    if amount <= 0:
        print(f"{coin} 잔고가 부족합니다.")
        return

    symbol = coin + "USDT"
    symbol_info = get_bitget_symbol_info(symbol)
    if not symbol_info:
        print("심볼 정보 조회 실패, 주문 진행 불가")
        return

    min_trade_amount = float(symbol_info.get("minTradeAmount", "1"))
    quantity_precision = int(symbol_info.get("quantityPrecision", "2"))

    if amount < min_trade_amount:
        print(f"주문 수량({amount})이 최소 주문 수량({min_trade_amount})보다 작아 주문 불가")
        return

    max_size = round(amount, quantity_precision)
    step = 10 ** (-quantity_precision)
    safe_size = max_size - step
    if safe_size < min_trade_amount:
        safe_size = min_trade_amount

    size_str = f"{safe_size:.{quantity_precision}f}"

    order_resp = place_spot_order(symbol=symbol, side="sell", orderType="market", force="normal", size=size_str)
    if order_resp.get("code") == "00000":
        print(f"BG매도 완료! 수량: {size_str} {coin}")
    else:
        print(f"매도 실패: {order_resp.get('msg')}")

##############################################
# 전체 잔고 조회 함수
##############################################
def 전체조회():
    print("\n=== 전체 잔고 조회를 시작합니다. ===\n")

    print("=== Binance Spot 잔고 조회 (3계정) ===")
    get_spot_balance_all()  # CR, LILAC, EX 모두 조회
    print()

    print("=== Bybit Unified 잔고 조회 ===")
    get_bybit_balance()
    print()

    print("=== Bitget Spot 잔고 조회 ===")
    print_bitget_balances()
    print()

    print("=== 전체 잔고 조회를 마쳤습니다. ===\n")


##############################################
# 메인 로직
##############################################

# 메뉴 안내
# 1.테스트 2.지갑조회 3.BN 4.BB매수 5.BB매도 6.BG매수 7.BG매도

테스트 = run_tests

while True:
    print("\n프로그램 실행 시 동작할 기능을 입력하시오")
    print("1.테스트 2.지갑조회 3.BN 4.BB매수 5.BB매도 6.BG매수 7.BG매도 8.종료")
    choice = input("입력: ").strip()

    if choice == '1':
        # 테스트
        if 테스트:
            테스트()

    elif choice == '2':
        # 지갑조회
        전체조회()

    elif choice == '3':
        # BN 거래소 선택 -> 계정 선택 -> 매수/매도 선택
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
            # BN 매수
            get_spot_balance_for_client(bn_client[0], bn_client[1])
            coin = input(f"{bn_client[1]} 계정 매수할 코인명: ").strip()
            if not coin:
                print("코인명을 입력하지 않아 매수를 취소합니다.")
                continue
            try:
                usdt_str = input(f"{coin.upper()} 매수할 USDT 수량: ").strip()
                usdt_amount = float(usdt_str)
            except:
                print("유효한 수량을 입력하지 않아 매수를 취소합니다.")
                continue
            buy_binance_coin_with_usdt_for_client(bn_client[0], bn_client[1], coin, usdt_amount)

        elif action_choice == '2':
            # BN 매도
            get_spot_balance_for_client(bn_client[0], bn_client[1])
            coin = input(f"{bn_client[1]} 계정 전액 매도할 코인명: ").strip()
            if coin:
                sell_all_binance_coin_for_client(bn_client[0], bn_client[1], coin)
            else:
                print("코인명을 입력하지 않아 매도를 취소합니다.")
        else:
            print("유효한 선택을 하지 않아 거래를 취소합니다.")

    elif choice == '4':
        # BB 매수
        # Bybit은 계정 1개
        get_bybit_balance()
        coin = input("BB거래소(매수) 매수할 코인명: ").strip()
        if not coin:
            print("코인명을 입력하지 않아 매수를 취소합니다.")
            continue

        try:
            usdt_str = input(f"{coin.upper()} 매수할 USDT 수량: ").strip()
            usdt_amount = float(usdt_str)
        except:
            print("유효한 수량을 입력하지 않아 매수를 취소합니다.")
            continue

        buy_bybit_coin_with_usdt(coin, usdt_amount)

    elif choice == '5':
        # BB 매도
        get_bybit_balance()
        coin = input("BB거래소(매도) 전액 매도할 코인명: ").strip()
        if coin:
            sell_all_bybit_coin(coin)
        else:
            print("코인명을 입력하지 않아 매도를 취소합니다.")

    elif choice == '6':
        # BG 매수
        print_bitget_balances()
        coin = input("BG거래소(매수) 매수할 코인명: ").strip()
        if not coin:
            print("코인명을 입력하지 않아 매수를 취소합니다.")
            continue

        try:
            usdt_str = input(f"{coin.upper()} 매수할 USDT 수량: ").strip()
            usdt_amount = float(usdt_str)
        except:
            print("유효한 수량을 입력하지 않아 매수를 취소합니다.")
            continue

        bitget_buy_coin_with_usdt(coin, usdt_amount)

    elif choice == '7':
        # BG 매도
        print_bitget_balances()
        coin = input("BG거래소(매도) 전액 매도할 코인명: ").strip()
        if coin:
            bitget_sell_all_coin(coin)
        else:
            print("코인명을 입력하지 않아 매도를 취소합니다.")

    elif choice == '8':
        # 종료
        print("프로그램을 종료합니다.")
        break

    else:
        print("해당 기능을 찾을 수 없습니다. (1=테스트/2=지갑조회/3=BN/4=BB매수/5=BB매도/6=BG매수/7=BG매도/8=종료)")
