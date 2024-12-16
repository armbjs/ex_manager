# ex_terminal_bot.py

from ex_manager import ExManager

COMMANDS_HELP = [
    ("notice_test", "테스트 공지 발행"),
    ("run_tests", "테스트 notices 실행"),
    ("buy.COIN.value", "COIN을 USDT로 value만큼 매수 (예: buy.BTC.100)"),
    ("sell.COIN", "COIN 전량 매도 (예: sell.ETH)"),
    ("show_trx.COIN", "COIN 거래내역 조회 (예: show_trx.BTC)"),
    ("show_pnl.COIN", "COIN 손익 평가 (예: show_pnl.BTC)"),
    ("show_bal", "모든 계좌 잔고 조회"),
    ("명령어 또는 help", "사용 가능한 명령어 목록 표시")
]

def print_commands():
    print("=== 사용 가능한 명령어 목록 ===\n")
    for cmd, desc in COMMANDS_HELP:
        print(f"{cmd} : {desc}")
    print()

def main():
    manager = ExManager()
    print("Terminal Command Interface")
    print("명령어를 입력해주세요. (종료하려면 exit 또는 quit)")
    print("명령어 목록을 보려면 'help' 또는 '명령어'를 입력하세요.\n")

    while True:
        cmd = input(">> ").strip()
        if cmd.lower() in ["exit", "quit"]:
            print("프로그램 종료.")
            break

        # 명령어 처리
        if cmd in ["help", "명령어"]:
            print_commands()

        elif cmd == "show_bal":
            result = manager.check_all_balances()
            print(result)

        elif cmd == "notice_test":
            result = manager.run_tests()  # run_tests 내부에서 notice 발행 로직을 실행한다고 가정
            print(result)

        elif cmd == "run_tests":
            # run_tests와 notice_test를 별도로 둘지 하나로 둘지 결정 필요
            # 여기서는 run_tests가 notice_test와 동일한 동작을 한다고 가정하거나
            # run_tests를 별도의 테스트 로직으로 구현 가능
            result = manager.run_tests()
            print(result)

        elif cmd.startswith("buy."):
            parts = cmd.split(".")
            if len(parts) == 3:
                c = parts[1]
                value_str = parts[2]
                try:
                    value = float(value_str)
                    result = manager.buy_all(c, value)
                    print(result)
                except:
                    print("invalid value")
            else:
                print("format: buy.COIN.value")

        elif cmd.startswith("sell."):
            parts = cmd.split(".")
            if len(parts) == 2:
                c = parts[1]
                result = manager.sell_all(c)
                print(result)
            else:
                print("format: sell.COIN")

        elif cmd.startswith("show_trx."):
            parts = cmd.split(".")
            if len(parts) == 2:
                c = parts[1]
                result = manager.show_trx(c)
                print(result)
            else:
                print("format: show_trx.COIN")

        elif cmd.startswith("show_pnl."):
            parts = cmd.split(".")
            if len(parts) == 2:
                c = parts[1]
                result = manager.show_profit_loss_per_account(c)
                print(result)
            else:
                print("format: show_pnl.COIN")

        else:
            print("No such feature.")
            print("명령어 목록을 보려면 'help' 또는 '명령어'를 입력하세요.")

if __name__ == "__main__":
    main()
