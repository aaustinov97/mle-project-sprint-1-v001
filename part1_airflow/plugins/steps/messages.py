from airflow.providers.telegram.hooks.telegram import TelegramHook

TOKEN = '7385279573:AAFqfjKQ_g1Y79Ychje-8oKYuOkwGK9wKlw'
TELEGRAM_CHAT_ID = '-1002257571456'

def send_telegram_success_message(context):
    hook = TelegramHook(token=TOKEN, chat_id=TELEGRAM_CHAT_ID)
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    })

def send_telegram_failure_message(context):
    hook = TelegramHook(token=TOKEN, chat_id=TELEGRAM_CHAT_ID)
    dag = context['dag']
    run_id = context['run_id']
    task_id = context['task_instance_key_str']
    
    message = f'Исполнение DAG {dag} с id={run_id} и task_id={task_id} прошло неудачно!'
    hook.send_message({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    })