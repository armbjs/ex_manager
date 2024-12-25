# ex_manager.py

import redis
import json
import time
import os
import sys
import logging
import math
import requests
import base64
from urllib.parse import urlencode
import hmac
import hashlib
import datetime
from io import StringIO

from apscheduler.schedulers.blocking import BlockingScheduler
from binance.client import Client
from binance.enums import *
from pybit.unified_trading import HTTP

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#############################################
# 상수 정의부
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


class ExManager:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_SERVER_ADDR,
            port=int(REDIS_SERVER_PORT),
            db=int(REDIS_SERVER_DB_INDEX),
            username=REDIS_SERVER_USERNAME,
            password=REDIS_SERVER_PASSWORD,
            ssl=REDIS_SERVER_SSL,
            decode_responses=True
        )

        self.binance_client_cr = Client(BINANCE_API_KEY_CR, BINANCE_API_SECRET_CR)
        self.binance_client_lilac = Client(BINANCE_API_KEY_LILAC, BINANCE_API_SECRET_LILAC)
        self.binance_client_ex = Client(BINANCE_API_KEY_EX, BINANCE_API_SECRET_EX)

        self.bybit_client = HTTP(
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
            testnet=False
        )

    def send_request(self, method, endpoint, params=None, body=None, need_auth=False):
        if params is None:
            params = {}
        if body is None:
            body = {}

        def get_timestamp():
            return str(int(time.time() * 1000))

        def sign(method, request_path, timestamp, body_str=""):
            message = timestamp + method + request_path + body_str
            signature = hmac.new(BITGET_SECRET_KEY.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
            signature_b64 = base64.b64encode(signature).decode()
            return signature_b64

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

    ##############################################
    # 현재가 가져오기 함수들
    ##############################################
    def get_current_price_binance(self, coin):
        symbol = coin.upper() + "USDT"
        ticker = self.binance_client_cr.get_symbol_ticker(symbol=symbol)
        return float(ticker['price'])

    def get_current_price_bybit(self, coin):
        symbol = coin.upper() + "USDT"
        resp = self.bybit_client.get_tickers(category="spot", symbol=symbol)
        if resp.get("retCode") == 0:
            ticker_list = resp.get("result", {}).get("list", [])
            if ticker_list and "lastPrice" in ticker_list[0]:
                return float(ticker_list[0]["lastPrice"])
        raise Exception(f"Failed to fetch Bybit current price for {symbol}")

    def get_current_price_bitget(self, coin):
        symbol = coin.upper() + "USDT"
        endpoint = "/api/v2/spot/market/tickers"
        params = {"symbol": symbol}
        resp = self.send_request("GET", endpoint, params=params, need_auth=False)
        if resp.get("code") == "00000":
            data_list = resp.get("data", [])
            if data_list and "lastPr" in data_list[0]:
                last_price_str = data_list[0]["lastPr"]
                return float(last_price_str)
        raise Exception(f"Failed to fetch Bitget current price for {symbol}")

    ##############################################
    # 테스트 공지 발행
    ##############################################
    def publish_test_notices(self):
        current_time = int(time.time() * 1000)
        coin_symbol = f"TST{current_time % 1000:03d}"

        # UPBIT 형식의 영문 공지사항
        upbit_notice_en1 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"Market Support for {coin_symbol}(TASDSDW), XRP(RIPPLE) (BTC, USDT Market)",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": "Trade",
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }
        upbit_notice_en1_json = json.dumps(upbit_notice_en1, ensure_ascii=False)
        self.redis_client.publish(REDIS_PUBLISH_CHANNEL_NAME_PREFIX, upbit_notice_en1_json)
        time.sleep(1)

        # UPBIT 형식의 영문 공지사항 (코인 순서 변경)
        upbit_notice_en2 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"Market Support for TASDSDW({coin_symbol}), RIPPLE(XRP) (BTC, USDT Market)",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": "Trade",
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }
        upbit_notice_en2_json = json.dumps(upbit_notice_en2, ensure_ascii=False)
        self.redis_client.publish(REDIS_PUBLISH_CHANNEL_NAME_PREFIX, upbit_notice_en2_json)
        time.sleep(1)

        # UPBIT 형식의 한글 공지사항 1
        upbit_notice_kr1 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"{coin_symbol}(테스트코인), XRP(RIPPLE) 신규 거래지원 안내 (BTC, USDT 마켓)",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": "Trade",
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }
        upbit_notice_kr1_json = json.dumps(upbit_notice_kr1, ensure_ascii=False)
        self.redis_client.publish(REDIS_PUBLISH_CHANNEL_NAME_PREFIX, upbit_notice_kr1_json)
        time.sleep(1)

        # UPBIT 형식의 한글 공지사항 2
        upbit_notice_kr2 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"테스트코인({coin_symbol}), 리플(XRP) 신규 거래지원 안내 (BTC, USDT 마켓)",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": "Trade",
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }
        upbit_notice_kr2_json = json.dumps(upbit_notice_kr2, ensure_ascii=False)
        self.redis_client.publish(REDIS_PUBLISH_CHANNEL_NAME_PREFIX, upbit_notice_kr2_json)
        time.sleep(1)

        # UPBIT 형식의 한글 공지사항 3
        upbit_notice_kr3 = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"테스트코인({coin_symbol}), 리플(XRP) KRW, USDT 마켓 디지털 자산 추가",
            "content": None,
            "exchange": "UPBIT",
            "url": "https://upbit.com/service_center/notice?id=4695",
            "category": "Trade",
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }
        upbit_notice_kr3_json = json.dumps(upbit_notice_kr3, ensure_ascii=False)
        self.redis_client.publish(REDIS_PUBLISH_CHANNEL_NAME_PREFIX, upbit_notice_kr3_json)
        time.sleep(1)

        # BITHUMB 형식의 공지사항
        bithumb_notice = {
            "type": "NOTICE",
            "action": "NEW",
            "title": f"[거래] {coin_symbol}(TestCoin), XRP(리플) 신규 거래지원 안내 (BTC, USDT 마켓)",
            "content": None,
            "exchange": "BITHUMB",
            "url": "https://feed.bithumb.com/notice/1645287",
            "category": None,
            "listedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "listedTs": current_time,
            "receivedTs": current_time + 100
        }
        bithumb_notice_json = json.dumps(bithumb_notice, ensure_ascii=False)
        self.redis_client.publish(REDIS_PUBLISH_CHANNEL_NAME_PREFIX, bithumb_notice_json)
        time.sleep(1)

        return f"Published multiple test notices for {coin_symbol}\n"


    ##############################################
    # Binance 관련 함수
    ##############################################
    def get_spot_balance_for_client(self, client, account_name):
        output = StringIO()
        try:
            account_info = client.get_account()
            balances = [
                asset for asset in account_info['balances']
                if float(asset['free']) > 0 or float(asset['locked']) > 0
            ]
            output.write(f"\n=== Binance Spot Wallet Balance ({account_name}) ===\n\n")
            if not balances:
                output.write("No balance.\n\n")
            else:
                for balance_item in balances:
                    coin_name = balance_item['asset']
                    free_amount = float(balance_item['free'])
                    locked_amount = float(balance_item['locked'])
                    output.write(f"{coin_name}: available: {free_amount}, locked: {locked_amount}\n")
                output.write("\n")
        except Exception as e:
            output.write(f"Error ({account_name}): {e}\n\n")
        return output.getvalue()

    def get_spot_balance_all(self):
        output = StringIO()
        output.write(self.get_spot_balance_for_client(self.binance_client_cr, "CR"))
        output.write(self.get_spot_balance_for_client(self.binance_client_lilac, "LILAC"))
        output.write(self.get_spot_balance_for_client(self.binance_client_ex, "EX"))
        return output.getvalue()

    def buy_binance_coin_usdt_raw(self, client, coin, usdt_amount):
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

    ###############################################################
    ################# 전량 매도 부분 절대 수정 금지 #################

    def sell_all_binance_coin_raw(self, client, coin):
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

    ################# 전량 매도 부분 절대 수정 금지 #################
    ###############################################################

    # 새로 추가 - 부분매도
    def sell_partial_binance_coin_raw(self, client, coin, percent):
        """
        coin 보유량의 percent%만 매도(소숫점은 버림)
        예: percent=50, balance=123 → 61개 매도
        """
        try:
            coin = coin.upper()
            balance_info = client.get_asset_balance(asset=coin)
            if not balance_info:
                return {"error": "Balance query failed"}
            balance_amount = float(balance_info['free'])

            if balance_amount <= 0:
                return {"error": "No balance to sell"}

            partial_amount = balance_amount * (percent / 100.0)
            quantity = float(int(partial_amount))  # 소숫점 버림
            if quantity <= 0:
                return {"error": f"Quantity too small after applying {percent}%."}

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

    def get_recent_trades_raw_binance(self, client, coin):
        try:
            symbol = (coin.upper() + "USDT")
            trades = client.get_my_trades(symbol=symbol, limit=200)
            return trades
        except Exception as e:
            return {"error": str(e)}

    def calculate_account_avg_buy_price(self, client, coin):
        trades = self.get_recent_trades_raw_binance(client, coin)
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

    def get_bid1_price_binance(self, coin):
        symbol = coin.upper() + "USDT"
        order_book = self.binance_client_cr.get_order_book(symbol=symbol)
        return float(order_book['bids'][0][0])  # 최우선 매수 호가

    ##############################################
    # Bybit 관련 함수
    ##############################################
    def get_symbol_filters(self, symbol):
        resp = self.bybit_client.get_instruments_info(category="spot", symbol=symbol)
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

    def adjust_quantity_to_step(self, qty, step, min_qty):
        adjusted = math.floor(qty / step) * step
        if adjusted < min_qty:
            return 0.0
        return adjusted

    def get_decimal_places(self, qty_step):
        if qty_step == 0:
            return 2
        if qty_step >= 1:
            return 0
        return abs(math.floor(math.log10(qty_step)))

    def buy_bybit_coin_usdt_raw(self, coin, usdt_amount):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            response = self.bybit_client.get_wallet_balance(accountType="UNIFIED")
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

            order_resp = self.bybit_client.place_order(
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

    # 절대 수정 금지: 전량매도
    def sell_all_bybit_coin_raw(self, coin):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            min_qty, qty_step = self.get_symbol_filters(symbol)

            response = self.bybit_client.get_wallet_balance(accountType="UNIFIED")
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

            sell_qty = self.adjust_quantity_to_step(coin_balance, qty_step, min_qty)
            if sell_qty <= 0:
                return {"error": "Quantity too small"}

            decimal_places = self.get_decimal_places(qty_step)
            qty_str = f"{sell_qty:.{decimal_places}f}"

            order_resp = self.bybit_client.place_order(
                category="spot",
                symbol=symbol,
                side="Sell",
                orderType="MARKET",
                qty=qty_str
            )
            return order_resp
        except Exception as e:
            return {"error": str(e)}
    ###########################################################

    # 새로 추가: 부분매도 (퍼센트) for Bybit
    def sell_partial_bybit_coin_raw(self, coin, percent):
        try:
            coin = coin.upper()
            symbol = coin + "USDT"
            min_qty, qty_step = self.get_symbol_filters(symbol)

            response = self.bybit_client.get_wallet_balance(accountType="UNIFIED")
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

            partial_amount = coin_balance * (percent / 100.0)
            # step 보정
            partial_amount_adj = self.adjust_quantity_to_step(partial_amount, qty_step, 0)
            final_qty = float(int(partial_amount_adj))  # 소숫점 버림

            if final_qty <= 0:
                return {"error": f"Quantity too small after applying {percent}%."}

            # min_qty 다시 체크
            if final_qty < min_qty:
                return {"error": "Quantity too small after step adjustment"}

            decimal_places = self.get_decimal_places(qty_step)
            qty_str = f"{final_qty:.{decimal_places}f}"

            order_resp = self.bybit_client.place_order(
                category="spot",
                symbol=symbol,
                side="Sell",
                orderType="MARKET",
                qty=qty_str
            )
            return order_resp
        except Exception as e:
            return {"error": str(e)}

    def get_recent_bybit_trades_raw(self, coin):
        try:
            symbol = (coin.upper() + "USDT")
            resp = self.bybit_client.get_executions(category="spot", symbol=symbol, limit=200)

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

    def calculate_bybit_avg_buy_price(self, coin):
        trades = self.get_recent_bybit_trades_raw(coin)
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

    def get_bid1_price_bybit(self, coin):
        symbol = coin.upper() + "USDT"
        response = self.bybit_client.get_orderbook(category="spot", symbol=symbol)
        if response.get("retCode") == 0:
            if response['result'] and response['result']['b']:
                return float(response['result']['b'][0][0])  # 최우선 매수 호가
            raise Exception(f"No bid data for {symbol}")
        raise Exception(f"Bybit API Error: {response.get('retMsg', 'Unknown error')}")

    ##############################################
    # Bitget 관련 함수
    ##############################################
    def check_spot_balance(self, coin=None, asset_type=None):
        endpoint = "/api/v2/spot/account/assets"
        params = {}
        if coin:
            params["coin"] = coin
        if asset_type:
            params["assetType"] = asset_type
        return self.send_request("GET", endpoint, params=params, need_auth=True)

    def place_spot_order(self, symbol, side, order_type, force, size, price=None, client_oid=None):
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
        result = self.send_request("POST", endpoint, body=body, need_auth=True)
        return result

    def get_bitget_symbol_info(self, symbol):
        endpoint = "/api/v2/spot/public/symbols"
        params = {}
        if symbol:
            params["symbol"] = symbol
        resp = self.send_request("GET", endpoint, params=params, need_auth=False)
        if resp.get("code") != "00000":
            return None

        products = resp.get("data", [])
        for p in products:
            if p.get("symbol") == symbol:
                return p
        return None

    def bitget_buy_coin_usdt_raw(self, coin, usdt_amount):
        coin = coin.upper()
        balance_data = self.check_spot_balance()
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
        order_resp = self.place_spot_order(symbol=symbol, side="buy", order_type="market", force="normal", size=str(usdt_to_use))
        return order_resp

    # 절대 수정 금지: 전량매도
    def bitget_sell_all_coin_raw(self, coin):
        coin = coin.upper()
        balance_data = self.check_spot_balance()
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
        symbol_info = self.get_bitget_symbol_info(symbol)
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
        order_resp = self.place_spot_order(symbol=symbol, side="sell", order_type="market", force="normal", size=size_str)
        return order_resp
    ###################################################################

    # 새로 추가: 부분매도 (퍼센트) for Bitget
    def bitget_sell_partial_coin_raw(self, coin, percent):
        coin = coin.upper()
        balance_data = self.check_spot_balance()
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

        partial_amount = amount * (percent / 100.0)
        partial_amount_floor = float(int(partial_amount))  # 소숫점 버림
        if partial_amount_floor <= 0:
            return {"error": f"Quantity too small after applying {percent}%"}

        symbol = coin + "USDT"
        symbol_info = self.get_bitget_symbol_info(symbol)
        if not symbol_info:
            return {"error": "Symbol info not found"}

        min_trade_amount = float(symbol_info.get("minTradeAmount", "1"))
        quantity_precision = int(symbol_info.get("quantityPrecision", "2"))

        if partial_amount_floor < min_trade_amount:
            return {"error": "Amount too small"}

        max_size = round(partial_amount_floor, quantity_precision)
        step = 10 ** (-quantity_precision)
        safe_size = max_size - step
        if safe_size < min_trade_amount:
            safe_size = min_trade_amount

        size_str = f"{safe_size:.{quantity_precision}f}"
        order_resp = self.place_spot_order(symbol=symbol, side="sell", order_type="market", force="normal", size=size_str)
        return order_resp

    def get_recent_bg_trades_raw(self, coin):
        try:
            symbol = (coin.upper() + "USDT")
            endpoint = "/api/v2/spot/trade/fills"
            params = {
                "symbol": symbol,
                "limit": "100"
            }

            resp = self.send_request("GET", endpoint, params=params, need_auth=True)
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

    def calculate_bg_avg_buy_price(self, coin):
        trades = self.get_recent_bg_trades_raw(coin)
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

    def get_bid1_price_bitget(self, coin):
        try:
            symbol = coin.upper() + "USDT"
            endpoint = "/api/v2/spot/market/orderbook"
            params = {"symbol": symbol, "type": "step0", "limit": "1"}
            response = self.send_request("GET", endpoint, params=params)
            if response.get("code") == "00000" and response.get("data") and response['data']['bids']:
                return float(response['data']['bids'][0][0])  # 최우선 매수 호가 반환
            raise Exception(f"Bitget API Error: {response.get('msg', 'Unknown error')}")
        except Exception as e:
            raise Exception(f"Failed to fetch Bitget bid1 price for {coin.upper()}: {str(e)}")


    ##############################################
    # 손익평가
    ##############################################
    def show_profit_loss_per_account(self, coin):
        output = StringIO()
        output.write("=== PnL Calculation Based on bid1 ===\n\n")

        bid1_binance = None
        bid1_bybit = None
        bid1_bitget = None

        # Binance bid1 price
        try:
            bid1_binance = self.get_bid1_price_binance(coin)
            output.write(f"Binance bid1 price: {bid1_binance}\n\n")
        except Exception as e:
            output.write(f"Failed to fetch Binance bid1 price for {coin}: {e}\n\n")

        # Bybit bid1 price
        try:
            bid1_bybit = self.get_bid1_price_bybit(coin)
            output.write(f"Bybit bid1 price: {bid1_bybit}\n\n")
        except Exception as e:
            output.write(f"Failed to fetch Bybit bid1 price for {coin}: {e}\n\n")

        # Bitget bid1 price
        try:
            bid1_bitget = self.get_bid1_price_bitget(coin)
            output.write(f"Bitget bid1 price: {bid1_bitget}\n\n")
        except Exception as e:
            output.write(f"Failed to fetch Bitget bid1 price for {coin}: {e}\n\n")

        output.write("=== Binance PnL ===\n\n")

        binance_accounts = [
            (self.binance_client_cr, "CR"),
            (self.binance_client_lilac, "LILAC"),
            (self.binance_client_ex, "EX")
        ]
        for client, acc_name in binance_accounts:
            try:
                avg_price = self.calculate_account_avg_buy_price(client, coin)
                if avg_price is not None and bid1_binance is not None:
                    pnl = bid1_binance - avg_price
                    pnl_percent = (pnl / avg_price) * 100.0
                    output.write(f"[BNS-{acc_name}] bid1_price: ${bid1_binance:.6f}, avg_price: ${avg_price:.6f}, pnl: {pnl_percent:.3f}%\n")
                else:
                    if avg_price is None:
                        output.write(f"[BNS-{acc_name}] no buy history\n")
                    else:
                        output.write(f"[BNS-{acc_name}] bid1 price unavailable\n")

                # ▼▼▼▼▼ [추가] Binance 보유 수량 * 현재 bid1 값 출력 ▼▼▼▼▼
                try:
                    balance_info = client.get_asset_balance(asset=coin.upper())
                    balance_amount = float(balance_info['free']) if balance_info and balance_info.get('free') else 0.0
                    if balance_amount > 0 and bid1_binance is not None:
                        current_value = balance_amount * bid1_binance
                        output.write(f"[BNS-{acc_name}] holding: {balance_amount:.6f} {coin.upper()} == ${current_value:.2f}\n\n")
                    else:
                        output.write(f"[BNS-{acc_name}] holding: 0 {coin.upper()}\n\n")
                except Exception as e2:
                    output.write(f"[BNS-{acc_name}] holding check error: {e2}\n\n")
                # ▲▲▲▲▲ [추가] Binance 보유 수량 * 현재 bid1 값 출력 ▲▲▲▲▲

            except Exception as e:
                output.write(f"[BNS-{acc_name}] Error calculating PnL: {e}\n\n")
        output.write("\n")

        output.write("=== Bybit PnL ===\n\n")
        try:
            avg_price_bybit = self.calculate_bybit_avg_buy_price(coin)
            if avg_price_bybit is not None and bid1_bybit is not None:
                pnl = bid1_bybit - avg_price_bybit
                pnl_percent = (pnl / avg_price_bybit) * 100.0
                output.write(f"[BBS] bid1_price: ${bid1_bybit:.6f}, avg_price: ${avg_price_bybit:.6f}, pnl: {pnl_percent:.3f}%\n")
            else:
                if avg_price_bybit is None:
                    output.write("[BBS] no buy history\n")
                else:
                    output.write("[BBS] bid1 price unavailable\n")

            # ▼▼▼▼▼ [추가] Bybit 보유 수량 * 현재 bid1 값 출력 ▼▼▼▼▼
            try:
                response = self.bybit_client.get_wallet_balance(accountType="UNIFIED")
                if response['retCode'] == 0:
                    coin_balance = 0.0
                    for account_item in response['result']['list']:
                        for c in account_item['coin']:
                            if c['coin'].upper() == coin.upper():
                                coin_balance = float(c.get('walletBalance', 0.0))
                                break
                    if coin_balance > 0 and bid1_bybit is not None:
                        current_value = coin_balance * bid1_bybit
                        output.write(f"[BBS] holding: {coin_balance:.6f} {coin.upper()} == ${current_value:.2f}\n\n")
                    else:
                        output.write(f"[BBS] holding: 0 {coin.upper()}\n\n")
                else:
                    output.write(f"[BBS] holding check error: {response['retMsg']}\n\n")
            except Exception as e2:
                output.write(f"[BBS] holding check error: {e2}\n\n")
            # ▲▲▲▲▲ [추가] Bybit 보유 수량 * 현재 bid1 값 출력 ▲▲▲▲▲

        except Exception as e:
            output.write(f"[BBS] Error calculating PnL: {e}\n")
        output.write("\n")

        output.write("=== Bitget PnL ===\n\n")
        try:
            avg_price_bg = self.calculate_bg_avg_buy_price(coin)
            if avg_price_bg is not None and bid1_bitget is not None:
                pnl = bid1_bitget - avg_price_bg
                pnl_percent = (pnl / avg_price_bg) * 100.0
                output.write(f"[BGS] bid1_price: ${bid1_bitget:.6f}, avg_price: ${avg_price_bg:.6f}, pnl: {pnl_percent:.3f}%\n")
            else:
                if avg_price_bg is None:
                    output.write("[BGS] no buy history\n")
                else:
                    output.write("[BGS] bid1_price unavailable\n")

            # ▼▼▼▼▼ [추가] Bitget 보유 수량 * 현재 bid1 값 출력 ▼▼▼▼▼
            try:
                res = self.check_spot_balance(coin=coin.upper())
                coin_balance = 0.0
                if res and res.get("code") == "00000":
                    for b in res.get("data", []):
                        if b.get("coin", "").upper() == coin.upper():
                            coin_balance = float(b.get("available", 0.0))
                            break
                if coin_balance > 0 and bid1_bitget is not None:
                    current_value = coin_balance * bid1_bitget
                    output.write(f"[BGS] holding: {coin_balance:.6f} {coin.upper()} == ${current_value:.2f}\n\n")
                else:
                    output.write(f"[BGS] holding: 0 {coin.upper()}\n\n")
            except Exception as e2:
                output.write(f"[BGS] holding check error: {e2}\n\n")
            # ▲▲▲▲▲ [추가] Bitget 보유 수량 * 현재 bid1 값 출력 ▲▲▲▲▲

        except Exception as e:
            output.write(f"[BGS] Error calculating PnL: {e}\n\n")
        output.write("\n")

        return output.getvalue()


    ##############################################
    # 체결내역 출력 함수
    ##############################################
    def print_trade_history(self, trades):
        output = StringIO()
        if isinstance(trades, dict) and trades.get("error"):
            output.write(str(trades) + "\n\n")
            return output.getvalue()

        if not trades or len(trades) == 0:
            output.write("no fills.\n\n")
            return output.getvalue()

        for t in trades:
            trade_time = datetime.datetime.fromtimestamp(t['time'] / 1000.0)
            side = "bid" if t['isBuyer'] else "ask"
            coin_name = t['symbol'].replace("USDT", "")
            qty = t['qty']
            price = t['price']
            output.write(f"{trade_time} {side} {qty} {coin_name} at {price}\n")
        output.write("\n")
        return output.getvalue()

    ##############################################
    # 전체 잔고 조회 함수
    ##############################################
    def check_all_balances(self):
        output = StringIO()
        output.write("\n=== All Balances ===\n\n")
        output.write("=== Binance Spot Balances (3acc) ===\n\n")
        output.write(self.get_spot_balance_all())

        output.write("=== Bybit Unified Balances ===\n\n")
        try:
            response = self.bybit_client.get_wallet_balance(accountType="UNIFIED")
            if response['retCode'] == 0:
                coins_found = False
                for account_item in response['result']['list']:
                    for c in account_item['coin']:
                        wallet_balance = float(c.get('walletBalance', 0))
                        if wallet_balance > 0:
                            output.write(f"{c['coin']}: balance: {wallet_balance}\n")
                            coins_found = True
                if not coins_found:
                    output.write("no balance\n")
            else:
                output.write(f"balance query failed: {response['retMsg']}\n")
        except Exception as e:
            output.write(f"error: {e}\n")
        output.write("\n")

        output.write("=== Bitget Spot Balances ===\n\n")
        res = self.check_spot_balance()
        if res and res.get("code") == "00000":
            data = res.get("data", [])
            if not data:
                output.write("no balance\n")
            else:
                coins_found = False
                for b in data:
                    coin = b.get("coin")
                    available = b.get("available")
                    if float(available) > 0:
                        output.write(f"{coin}: available: {available}\n")
                        coins_found = True
                if not coins_found:
                    output.write("no balance\n")
        else:
            output.write("Bitget balance query failed\n")

        output.write("\n=== All balances end ===\n")
        return output.getvalue()

    ##############################################
    # 전체매수/전체매도
    ##############################################
    def buy_all(self, coin, usdt_amount):
        output = StringIO()
        output.write("=== buy all ===\n\n")
        bn_cr_result = self.buy_binance_coin_usdt_raw(self.binance_client_cr, coin, usdt_amount)
        bn_lilac_result = self.buy_binance_coin_usdt_raw(self.binance_client_lilac, coin, usdt_amount)
        bn_ex_result = self.buy_binance_coin_usdt_raw(self.binance_client_ex, coin, usdt_amount)

        bb_result = self.buy_bybit_coin_usdt_raw(coin, usdt_amount)
        bg_result = self.bitget_buy_coin_usdt_raw(coin, usdt_amount)

        output.write("[BNS - CR]\n")
        output.write(str(bn_cr_result) + "\n\n")
        output.write("[BNS - LILAC]\n")
        output.write(str(bn_lilac_result) + "\n\n")
        output.write("[BNS - EX]\n")
        output.write(str(bn_ex_result) + "\n\n")
        output.write("[BBS]\n")
        output.write(str(bb_result) + "\n\n")
        output.write("[BGS]\n")
        output.write(str(bg_result) + "\n\n")
        return output.getvalue()

    # 절대 수정 금지(기존 전량매도)
    def sell_all(self, coin):
        output = StringIO()
        output.write("=== sell all ===\n\n")
        bn_cr_result = self.sell_all_binance_coin_raw(self.binance_client_cr, coin)
        bn_lilac_result = self.sell_all_binance_coin_raw(self.binance_client_lilac, coin)
        bn_ex_result = self.sell_all_binance_coin_raw(self.binance_client_ex, coin)

        bb_result = self.sell_all_bybit_coin_raw(coin)
        bg_result = self.bitget_sell_all_coin_raw(coin)

        output.write("[BNS - CR]\n")
        output.write(str(bn_cr_result) + "\n\n")
        output.write("[BNS - LILAC]\n")
        output.write(str(bn_lilac_result) + "\n\n")
        output.write("[BNS - EX]\n")
        output.write(str(bn_ex_result) + "\n\n")
        output.write("[BBS]\n")
        output.write(str(bb_result) + "\n\n")
        output.write("[BGS]\n")
        output.write(str(bg_result) + "\n\n")
        return output.getvalue()
    ##############################################
    
    # 새로 추가: 부분매도 (퍼센트) - 모든 거래소
    def sell_partial_all(self, coin, percent):
        """
        coin 보유량의 percent%만 매도.
        전량매도 함수(sell_all)는 절대 수정 불가이므로, 별도의 함수를 만듬.
        """
        output = StringIO()
        output.write(f"=== sell partial ({percent}%) ===\n\n")

        # Binance 3 계정
        bn_cr_result = self.sell_partial_binance_coin_raw(self.binance_client_cr, coin, percent)
        bn_lilac_result = self.sell_partial_binance_coin_raw(self.binance_client_lilac, coin, percent)
        bn_ex_result = self.sell_partial_binance_coin_raw(self.binance_client_ex, coin, percent)

        # Bybit
        bb_result = self.sell_partial_bybit_coin_raw(coin, percent)

        # Bitget
        bg_result = self.bitget_sell_partial_coin_raw(coin, percent)

        output.write("[BNS - CR]\n")
        output.write(str(bn_cr_result) + "\n\n")
        output.write("[BNS - LILAC]\n")
        output.write(str(bn_lilac_result) + "\n\n")
        output.write("[BNS - EX]\n")
        output.write(str(bn_ex_result) + "\n\n")
        output.write("[BBS]\n")
        output.write(str(bb_result) + "\n\n")
        output.write("[BGS]\n")
        output.write(str(bg_result) + "\n\n")
        return output.getvalue()

    ##############################################
    # 거래내역 조회
    ##############################################
    def show_trx(self, coin):
        output = StringIO()
        c = coin.upper()
        output.write(f"=== Transaction History for {c} ===\n\n")

        output.write(f"=== Binance (CR) [{c}] ===\n\n")
        cr_trades = self.get_recent_trades_raw_binance(self.binance_client_cr, c)
        output.write(self.print_trade_history(cr_trades))

        output.write(f"=== Binance (LILAC) [{c}] ===\n\n")
        lilac_trades = self.get_recent_trades_raw_binance(self.binance_client_lilac, c)
        output.write(self.print_trade_history(lilac_trades))

        output.write(f"=== Binance (EX) [{c}] ===\n\n")
        ex_trades = self.get_recent_trades_raw_binance(self.binance_client_ex, c)
        output.write(self.print_trade_history(ex_trades))

        output.write(f"=== Bybit [{c}] ===\n\n")
        bb_trades = self.get_recent_bybit_trades_raw(c)
        output.write(self.print_trade_history(bb_trades))

        output.write(f"=== Bitget [{c}] ===\n\n")
        bg_trades = self.get_recent_bg_trades_raw(c)
        output.write(self.print_trade_history(bg_trades))

        return output.getvalue()

    ##############################################
    # 명령어 헬프
    ##############################################
    COMMANDS_HELP = [
        ("test, notice_test", "테스트 공지 발행"),
        ("buy.COIN.value", "COIN을 USDT로 value만큼 매수 (예: buy.BTC.100)"),
        ("sell.COIN", "COIN 전량 매도 (예: sell.ETH)"),
        ("sell.COIN.percent", "COIN 부분 매도 (예: sell.XRP.50 -> 보유량의 50% 매도)"),
        ("show_trx.COIN, trx.COIN", "COIN 거래내역 조회 (예: show_trx.BTC 혹은 trx.BTC)"),
        ("show_pnl.COIN, pnl.COIN", "COIN 손익 평가 (예: show_pnl.BTC 혹은 pnl.BTC)"),
        ("show_bal, bal", "모든 계좌 잔고 조회"),
        ("명령어, help, ?, h", "사용 가능한 명령어 목록 표시")
    ]

    def execute_command(self, text):
        """
        기존 코드에서 print(...)만 하던 부분을
        buffer.write(...)로 바꾸어 텔레그램에도 전송되도록 수정
        """
        import sys
        from io import StringIO

        # 1) 표준출력을 StringIO로 교체
        old_stdout = sys.stdout
        buffer = StringIO()
        sys.stdout = buffer

        try:
            original_text = text.strip()
            text_lower = original_text.lower()

            # 디버깅용 - 콘솔에서 확인하고 싶다면 남겨둠
            print(f"Received command: {original_text}\n")

            # notice_test, test
            if text_lower in ["notice_test", "test"]:
                # 기존: print(self.publish_test_notices())
                # 수정: buffer에 기록
                result_str = self.publish_test_notices()
                buffer.write(result_str)

            # buy.COIN.value
            elif text_lower.startswith("buy."):
                parts = text_lower.split(".")
                if len(parts) == 3:
                    coin = parts[1]
                    try:
                        value = float(parts[2])
                        # 기존: print(self.buy_all(coin, value))
                        result_str = self.buy_all(coin, value)
                        buffer.write(result_str)
                    except:
                        buffer.write("invalid value\n")
                else:
                    buffer.write("format: buy.COIN.value\n")

            # sell.COIN.PERCENT (부분매도)
            elif text_lower.startswith("sell.") and len(text_lower.split(".")) == 3:
                parts = text_lower.split(".")
                coin = parts[1]
                try:
                    percent = float(parts[2])
                    # 기존: print(self.sell_partial_all(coin, percent))
                    result_str = self.sell_partial_all(coin, percent)
                    buffer.write(result_str)
                except:
                    buffer.write("invalid percent\n")

            # sell.COIN (전량매도)
            elif text_lower.startswith("sell.") and len(text_lower.split(".")) == 2:
                parts = text_lower.split(".")
                coin = parts[1]
                # 기존: print(self.sell_all(coin))
                result_str = self.sell_all(coin)
                buffer.write(result_str)

            # show_trx.COIN, trx.COIN
            elif text_lower.startswith("show_trx.") or text_lower.startswith("trx."):
                parts = text_lower.split(".")
                if len(parts) == 2:
                    c = parts[1]
                    # 기존: print(self.show_trx(c))
                    result_str = self.show_trx(c)
                    buffer.write(result_str)
                else:
                    buffer.write("format: show_trx.COIN or trx.COIN\n")

            # show_pnl.COIN, pnl.COIN
            elif text_lower.startswith("show_pnl.") or text_lower.startswith("pnl."):
                parts = text_lower.split(".")
                if len(parts) == 2:
                    c = parts[1]
                    # 기존: print(self.show_profit_loss_per_account(c))
                    result_str = self.show_profit_loss_per_account(c)
                    buffer.write(result_str)
                else:
                    buffer.write("format: show_pnl.COIN or pnl.COIN\n")

            # show_bal, bal
            elif text_lower in ["show_bal", "bal"]:
                # 기존: print(self.check_all_balances())
                result_str = self.check_all_balances()
                buffer.write(result_str)

            # 명령어, help, ?, h
            elif text_lower in ["명령어", "help", "?", "h"]:
                # 기존: print("=== 사용 가능한 명령어 목록 ===\n")
                #       for cmd, desc in self.COMMANDS_HELP:
                #           print(f"{cmd} : {desc}")
                #       print()
                buffer.write("=== 사용 가능한 명령어 목록 ===\n\n")
                for cmd, desc in self.COMMANDS_HELP:
                    buffer.write(f"{cmd} : {desc}\n")
                buffer.write("\n")
                buffer.write("\nlisting-handler-walsook v0.1.0\n\n")

            else:
                # 기존: print("No such feature.\n")
                buffer.write("No such feature.\n")

        finally:
            # 2) stdout 복구
            sys.stdout = old_stdout

        # 3) 최종 반환: buffer에 쌓인 문자열 → 텔레그램 메시지로 전송 가능
        return buffer.getvalue()
