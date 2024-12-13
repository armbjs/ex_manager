#############################################
# 1번 파트 시작 (테스트 notices 발행 관련 코드)
##############################################
try:
    import redis
    import json
    import time
    import os
    import sys
    import logging
    from apscheduler.schedulers.blocking import BlockingScheduler

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
# 1번 파트 끝
##############################################


##############################################
# 2번 파트 시작 (Binance 매도/매수 관련 코드)
##############################################
try:
    from binance.client import Client
    from binance.enums import *
    import math

    # LILAC_BNS API Key
    api_key = 'CUvOhge0SRuYgon2edHchbOVIX3d70GQCevrvRMPNsL5LJ1mTdtVKFfB3FDu9hay'
    api_secret = 'rhHey3FnPHsoW1lhWgYmfdZmJJVkqTxL1kjBa6Y4TrBgV0AyNk2EIPqRaR8rNykf'

    binance_client = Client(api_key, api_secret)

    def sell_all_binance_coin(coin):
        try:
            coin = coin.upper()
            balance_info = binance_client.get_asset_balance(asset=coin)
            if not balance_info:
                print(f"{coin} 잔고 조회 실패")
                return
            balance = float(balance_info['free'])

            if balance <= 0:
                print(f"판매할 {coin}가 없습니다.")
                return

            quantity = float(int(balance))

            symbol = coin + "USDT"
            order = binance_client.create_order(
                symbol=symbol,
                side=SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )

            print(f"BN매도 완료! 수량: {quantity} {coin}")
            # 매도 후 잔고 조회
            get_spot_balance()
        except Exception as e:
            print(f"에러 발생: {e}")

    def buy_all_binance_coin_with_usdt(coin):
        try:
            coin = coin.upper()
            # USDT 잔고 조회
            usdt_info = binance_client.get_asset_balance(asset='USDT')
            if not usdt_info:
                print("USDT 잔고 조회 실패")
                return
            usdt_balance = float(usdt_info['free'])
            if usdt_balance <= 0:
                print("USDT 잔고가 부족합니다.")
                return

            symbol = coin + "USDT"

            # ticker를 조회해 현재 가격 획득
            ticker = binance_client.get_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])

            # 소수점 둘째 자리에서 버림
            usdt_to_use = math.floor(usdt_balance * 100) / 100.0
            if usdt_to_use <= 0:
                print("USDT 금액이 너무 적어서 주문할 수 없습니다.")
                return

            quantity = usdt_to_use / price
            quantity = float(int(quantity))  # 정수화

            if quantity <= 0:
                print("USDT 금액이 너무 적어서 주문할 수 없습니다.")
                return

            order = binance_client.create_order(
                symbol=symbol,
                side=SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )
            print(f"BN매수 완료! {quantity} {coin} 매수 완료.")
            # 매수 후 잔고 조회
            get_spot_balance()
        except Exception as e:
            print(f"에러 발생: {e}")

except Exception:
    pass
##############################################
# 2번 파트 끝
##############################################


##############################################
# 3번 파트 시작 (Binance 스팟 잔고 조회 관련 코드)
##############################################
try:
    from binance.client import Client as Client_3

    api_key_3 = 'nn0r9JF9CjJ2vd1lsCbyqmmcmK5HWGX1jqL0ukuoZIJhDwRLy0Q1VOd2mLcee9Ur'
    api_secret_3 = '8UkN9v0ZIhAaVd23lcp1IKbyj01WFCxSDkMR88lhRMp05I5qySVyoO9xDMUfkJuH'

    client_3 = Client_3(api_key_3, api_secret_3)

    def get_spot_balance():
        try:
            account = client_3.get_account()
            balances = [
                asset for asset in account['balances']
                if float(asset['free']) > 0 or float(asset['locked']) > 0
            ]

            print("\n=== Binance 스팟 지갑 잔고 ===")
            if not balances:
                print("잔고가 없습니다.")
            for balance in balances:
                coin = balance['asset']
                free = float(balance['free'])
                locked = float(balance['locked'])
                print(f"{coin}: 사용 가능: {free}, 잠금: {locked}")
        except Exception as e:
            print(f"에러 발생: {e}")
except Exception:
    pass
##############################################
# 3번 파트 끝
##############################################


##############################################
# 4번 파트 (Bybit 스팟 잔고 조회 및 매도/매수 관련 코드)
##############################################
try:
    import math
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
                # 매도 후 잔고 조회
                get_bybit_balance()
            else:
                print(f"매도 실패: {order_resp['retMsg']}")
        except Exception as e:
            print(f"에러 발생: {e}")

    def buy_all_bybit_coin_with_usdt(coin):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            # USDT 잔고 조회
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

            # 소수점 2자리에서 버림
            usdt_to_use = math.floor(usdt_balance * 100) / 100.0
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
                # 매수 후 잔고 조회
                get_bybit_balance()
            else:
                print(f"매수 실패: {order_resp['retMsg']}")
        except Exception as e:
            print(f"에러 발생: {e}")

except Exception as e:
    print(f"Bybit 관련 코드에서 에러 발생: {e}")


###############################################
# 5번 파트 (Bitget 스팟 잔고 조회 및 매도/매수)
##############################################

import time
import hmac
import hashlib
import json
import requests
import base64
from urllib.parse import urlencode
import math

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

def get_spot_account_info():
    endpoint = "/api/v2/spot/account/info"
    result = send_request("GET", endpoint, need_auth=True)
    print("Spot 계정 정보 조회 결과:")
    print(json.dumps(result, indent=2))

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
    # 주문 결과를 직접 print하지 않고 호출하는 함수쪽에서 처리
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

def bitget_buy_all_coin_with_usdt(coin):
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

    # 소수점 2자리에서 버림
    usdt_to_use = math.floor(usdt_balance * 100) / 100.0
    if usdt_to_use <= 0:
        print("USDT 금액이 너무 적어서 주문할 수 없습니다.")
        return

    symbol = coin + "USDT"
    order_resp = place_spot_order(symbol=symbol, side="buy", orderType="market", force="normal", size=str(usdt_to_use))
    if order_resp.get("code") == "00000":
        print(f"BG매수 완료! 약 {usdt_to_use} USDT 상당의 {coin} 매수")
        print_bitget_balances()
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
        print_bitget_balances()
    else:
        print(f"매도 실패: {order_resp.get('msg')}")


##############################################
# 메인 부분 수정
##############################################
if __name__ == "__main__":
    commands = {}
    if 'run_tests' in globals():
        commands['테스트'] = run_tests

    # Binance 명령어
    def BN매도():
        get_spot_balance()
        coin = input("전액 매도할 코인명을 입력해주세요: ").strip()
        if coin:
            sell_all_binance_coin(coin)
        else:
            print("코인명을 입력하지 않아 매도를 취소합니다.")

    def BN매수():
        get_spot_balance()
        coin = input("전액 매수할 코인명을 입력해주세요: ").strip()
        if coin:
            buy_all_binance_coin_with_usdt(coin)
        else:
            print("코인명을 입력하지 않아 매수를 취소합니다.")

    if 'sell_all_binance_coin' in globals():
        commands['BN매도'] = BN매도
    if 'get_spot_balance' in globals():
        commands['BN조회'] = get_spot_balance
    if 'buy_all_binance_coin_with_usdt' in globals():
        commands['BN매수'] = BN매수

    # Bybit 명령어
    def BB매도():
        get_bybit_balance()
        coin = input("전액 매도할 코인명을 입력해주세요: ").strip()
        if coin:
            sell_all_bybit_coin(coin)
        else:
            print("코인명을 입력하지 않아 매도를 취소합니다.")

    def BB매수():
        get_bybit_balance()
        coin = input("전액 매수할 코인명을 입력해주세요: ").strip()
        if coin:
            buy_all_bybit_coin_with_usdt(coin)
        else:
            print("코인명을 입력하지 않아 매수를 취소합니다.")

    if 'get_bybit_balance' in globals():
        commands['BB조회'] = get_bybit_balance
    commands['BB매도'] = BB매도
    commands['BB매수'] = BB매수

    # Bitget 명령어
    def BG매도():
        res = check_spot_balance()
        if res and res.get("code") == "00000":
            print("\n=== Bitget Spot 지갑 잔고 조회 ===")
            data = res.get("data", [])
            if not data:
                print("잔고가 없습니다.")
            else:
                for b in data:
                    print(f"{b['coin']}: 사용 가능: {b['available']}")
        coin = input("전액 매도할 코인명을 입력해주세요: ").strip()
        if coin:
            bitget_sell_all_coin(coin)
        else:
            print("코인명을 입력하지 않아 매도를 취소합니다.")

    def BG매수():
        res = check_spot_balance()
        if res and res.get("code") == "00000":
            print("\n=== Bitget Spot 지갑 잔고 조회 ===")
            data = res.get("data", [])
            if not data:
                print("잔고가 없습니다.")
            else:
                for b in data:
                    print(f"{b['coin']}: 사용 가능: {b['available']}")
        coin = input("전액 매수할 코인명을 입력해주세요: ").strip()
        if coin:
            bitget_buy_all_coin_with_usdt(coin)
        else:
            print("코인명을 입력하지 않아 매수를 취소합니다.")

    if 'check_spot_balance' in globals():
        commands['BG조회'] = lambda: print_bitget_balances()
    commands['BG매도'] = BG매도
    commands['BG매수'] = BG매수
    if 'get_spot_account_info' in globals():
        commands['계정조회'] = get_spot_account_info

    # 전체조회 명령어
    def 전체조회():
        print("\n=== 전체 잔고 조회를 시작합니다. ===\n")

        if 'get_spot_balance' in globals():
            print("=== Binance Spot 잔고 조회 ===")
            get_spot_balance()
            print()

        if 'get_bybit_balance' in globals():
            print("=== Bybit Unified 잔고 조회 ===")
            get_bybit_balance()
            print()

        if 'check_spot_balance' in globals():
            print("=== Bitget Spot 잔고 조회 ===")
            print_bitget_balances()
            print()

        if 'get_spot_account_info' in globals():
            print("=== Bitget Spot 계정 정보 조회 ===")
            get_spot_account_info()
            print()

        print("=== 전체 잔고 조회를 마쳤습니다. ===\n")

    commands['전체조회'] = 전체조회

    while True:
        mode = input("프로그램 실행 시 동작할 기능을 입력하시오 (테스트/전체조회/BN매도/BN매수/BN조회/BB매도/BB매수/BB조회/BG매도/BG매수/BG조회/계정조회/종료): ").strip()
        if mode.lower() == '종료':
            print("프로그램을 종료합니다.")
            break

        if mode in commands:
            commands[mode]()
        else:
            print("해당 기능을 찾을 수 없습니다. (테스트/전체조회/BN매도/BN매수/BN조회/BB매도/BB매수/BB조회/BG매도/BG매수/BG조회/계정조회/종료)")
