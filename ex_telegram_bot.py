# ex_telegram_bot.py
from telegram import Update
from telegram.ext import Updater, MessageHandler, Filters, CallbackContext
from ex_manager import ExManager
import tempfile

# Telegram Bot Token
BOT_TOKEN = "7924311034:AAF2wYNL1B_K2QO6_qdLBojmdkN3VUTIzZw"

manager = ExManager()

def handle_message(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    result = manager.execute_command(text)
    
    # 텔레그램 메시지 최대 길이 기준 (약 4096자이므로 안전하게 4000자로 세팅)
    MAX_LENGTH = 4000

    if len(result) > MAX_LENGTH:
        # 너무 긴 경우 텍스트를 파일로 저장한 뒤 파일 전송
        with tempfile.NamedTemporaryFile(prefix="output_", suffix=".txt", delete=False, mode='w', encoding='utf-8') as f:
            f.write(result)
            f.flush()
            file_path = f.name
        
        # 안내 문구 전송
        update.message.reply_text("결과가 너무 길어 파일로 첨부합니다. 아래 파일을 다운로드해주세요.")
        
        # 파일 전송
        with open(file_path, 'rb') as f:
            context.bot.send_document(chat_id=update.effective_chat.id, document=f)
    else:
        # 길이가 적당한 경우 그냥 텍스트로 전송
        if result.strip():
            update.message.reply_text(result)

def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_message))

    print("Telegram Bot started. Waiting for commands...\n")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
