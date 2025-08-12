import datetime
from config.settings import HORARIO_PERMITIDO

def dentro_janela_permitida(now=None):
    now = now or datetime.datetime.now()
    if now.weekday() not in HORARIO_PERMITIDO['dias']:
        return False
    return HORARIO_PERMITIDO['hora_inicio'] <= now.hour < HORARIO_PERMITIDO['hora_fim']

def proxima_janela_inicio(after=None):
    after = after or datetime.datetime.now()
    cur = after.replace(minute=0, second=0, microsecond=0)
    start_hour = HORARIO_PERMITIDO['hora_inicio']

    if cur.weekday() in HORARIO_PERMITIDO['dias'] and cur.hour < start_hour:
        return cur.replace(hour=start_hour).timestamp()

    for i in range(1, 8):
        next_day = cur + datetime.timedelta(days=i)
        if next_day.weekday() in HORARIO_PERMITIDO['dias']:
            return next_day.replace(hour=start_hour).timestamp()
            
    return (cur + datetime.timedelta(days=1)).replace(hour=start_hour).timestamp()