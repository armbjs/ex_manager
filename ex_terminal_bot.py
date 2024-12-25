# ex_terminal_bot.py

from ex_manager import ExManager

def main():
    # ExManager 인스턴스 생성
    manager = ExManager()

    print("Terminal Command Interface")
    print("명령어를 입력해주세요. (종료하려면 exit 또는 quit)")
    print("명령어 목록을 보려면 'help' 또는 '명령어'를 입력하세요.\n")

    while True:
        # 1) 사용자 입력
        cmd = input(">> ").strip()

        # 2) 종료 명령 체크
        if cmd.lower() in ["exit", "quit"]:
            print("프로그램 종료.")
            break

        # 3) ex_manager.py의 execute_command() 호출
        result = manager.execute_command(cmd)

        # 4) 결과가 있다면 출력
        #    (execute_command()가 반환한 문자열을 그대로 터미널에 표시)
        if result.strip():
            print(result)

if __name__ == "__main__":
    main()
