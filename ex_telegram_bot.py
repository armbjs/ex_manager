"""
ex_telegram_bot.py
==================

이 스크립트는 Telegram Bot을 이용해 사용자의 메시지를 받아,
ex_manager.py 내부의 ExManager 클래스로 명령어를 전달하고,
그 결과를 텔레그램 메시지 또는 파일 형태로 응답하는 역할을 담당합니다.
"""

from telegram import Update
from telegram.ext import Updater, MessageHandler, Filters, CallbackContext
from ex_manager import ExManager
import tempfile

# Telegram Bot Token
BOT_TOKEN = "7924311034:AAF2wYNL1B_K2QO6_qdLBojmdkN3VUTIzZw"

# ExManager 인스턴스 생성
manager = ExManager()

def handle_message(update: Update, context: CallbackContext):
    """
    사용자가 텔레그램에 입력한 텍스트 메시지를 받아서,
    manager.execute_command()로 처리한 뒤 결과를 전송한다.
    """
    # 1) 유저 입력 텍스트 가져오기
    text = update.message.text.strip()

    # 2) ExManager의 execute_command() 호출 → 문자열 결과 획득
    #    만약 내부 로직상 None이 반환될 수도 있으므로 방어 로직 추가
    result = manager.execute_command(text)
    if result is None:
        # None일 경우 빈 문자열로 대체
        result = ""

    # 3) 텔레그램 메시지 최대 길이 기준 (약 4096자)이 있으므로, 4000자로 제한
    MAX_LENGTH = 4000

    # 4) 만약 결과 문자열이 4000자 이상이라면, 파일로 저장하여 전송
    if len(result) > MAX_LENGTH:
        # 임시 파일에 결과 쓰기 (delete=False로 해야 전송까지 파일이 유지됨)
        with tempfile.NamedTemporaryFile(prefix="output_", suffix=".txt", delete=False, mode='w', encoding='utf-8') as f:
            f.write(result)
            f.flush()
            file_path = f.name
        
        # 사용자에게 안내 문구 전송
        update.message.reply_text("결과가 너무 길어 파일로 첨부합니다. 아래 파일을 다운로드해주세요.")
        
        # 파일 전송
        with open(file_path, 'rb') as f:
            context.bot.send_document(chat_id=update.effective_chat.id, document=f)
    else:
        # 길이가 적당한 경우, 단순 텍스트 메시지로 응답
        # result.strip()가 비어있지 않은 경우에만 전송
        if result.strip():
            update.message.reply_text(result)


def main():
    """
    Bot 메인 함수:
    1) Updater를 이용해 Telegram 서버에 연결
    2) MessageHandler를 등록하여 모든 텍스트 메시지 핸들
    3) 무한루프(업데이트 감시) 시작
    """
    # 1) Updater 초기화
    updater = Updater(BOT_TOKEN, use_context=True)

    # 2) Dispatcher에 메시지 핸들러 등록
    dp = updater.dispatcher
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_message))

    # 3) 봇 시작 대기
    print("Telegram Bot started. Waiting for commands...\n")
    updater.start_polling()

    # 4) 종료 시그널(IDLE) 처리 (Ctrl+C나 다른 종료 이벤트 대기)
    updater.idle()


if __name__ == "__main__":
    main()
