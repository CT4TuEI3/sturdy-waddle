from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
import pandas as pd
from typing import Optional
import io

app = FastAPI()

# Ссылка на публичную Google Таблицу (CSV экспорт)
SHEET_ID = "1JaL7-otunC3ERFqHM3UYDtAo-aM3ad_R"
# Используем формат export вместо gviz для получения всех данных
GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"


def get_sheet_data() -> pd.DataFrame:
    """Загружает CSV данные из Google Таблицы"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/csv,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://docs.google.com/"
    }
    session = requests.Session()
    session.headers.update(headers)
    response = session.get(GOOGLE_SHEET_URL, timeout=30.0, allow_redirects=True)
    response.raise_for_status()
    # Убеждаемся, что используем правильную кодировку UTF-8
    response.encoding = 'utf-8'
    csv_data = io.StringIO(response.text)
    # Используем quoting=1 (QUOTE_ALL) для правильной обработки кавычек в CSV
    try:
        df = pd.read_csv(csv_data, header=None, quoting=1, on_bad_lines='skip', engine='python')
    except:
        # Если не получилось, пробуем без quoting
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, header=None, on_bad_lines='skip', engine='python')
    return df


def find_group_column(df: pd.DataFrame, group_name: str) -> Optional[int]:
    """Находит столбец с указанной группой"""
    # Строка 4 (индекс 4) содержит названия групп
    header_row = df.iloc[4]
    
    # Группы находятся в нечетных столбцах: 3, 5, 7, 9, 11... (индексы 3, 5, 7, 9, 11...)
    # Аудитории в четных: 4, 6, 8, 10... (индексы 4, 6, 8, 10...)
    for col_idx in range(3, len(header_row), 2):  # Начинаем с 3, шаг 2
        if col_idx < len(header_row):
            cell_value = str(header_row.iloc[col_idx]).strip()
            if cell_value == group_name:
                return col_idx
    
    return None


def parse_schedule(df: pd.DataFrame, group_col: int) -> list:
    """Парсит расписание для найденной группы"""
    schedule = []
    current_day = None
    current_lessons = []
    
    # Начинаем с строки 5 (индекс 5), т.к. строки 0-4 - заголовки
    for row_idx in range(5, len(df)):
        day_cell = str(df.iloc[row_idx, 0]).strip()  # Столбец A (индекс 0)
        pair_num = str(df.iloc[row_idx, 1]).strip()  # Столбец B (индекс 1)
        time = str(df.iloc[row_idx, 2]).strip()  # Столбец C (индекс 2)
        subject = str(df.iloc[row_idx, group_col]).strip()  # Предмет под группой
        room_col = group_col + 1  # Аудитория в следующем столбце
        room = str(df.iloc[row_idx, room_col]).strip() if room_col < len(df.columns) else ""
        
        # Если в ячейке дня есть значение (не пустое и не nan), обновляем текущий день
        if day_cell and day_cell != "nan" and day_cell != "":
            # Если был предыдущий день с уроками, сохраняем его
            if current_day and current_lessons:
                schedule.append({
                    "day": current_day,
                    "lessons": current_lessons
                })
            current_day = day_cell
            current_lessons = []
        
        # Пропускаем строки без дня (если еще не нашли первый день)
        if not current_day:
            continue
        
        # Добавляем пару, если есть предмет
        if subject and subject != "nan" and subject != "":
            try:
                pair_number = int(pair_num) if pair_num and pair_num != "nan" and pair_num != "" else None
            except (ValueError, TypeError):
                pair_number = None
            
            # Очищаем room от nan
            room_clean = room if room and room != "nan" else ""
            time_clean = time if time and time != "nan" else ""
            
            lesson = {
                "pair_number": pair_number,
                "subject": subject,
                "room": room_clean,
                "time": time_clean
            }
            current_lessons.append(lesson)
    
    # Добавляем последний день
    if current_day and current_lessons:
        schedule.append({
            "day": current_day,
            "lessons": current_lessons
        })
    
    return schedule


@app.get("/schedule")
async def get_schedule(group: str):
    """Возвращает расписание для указанной группы"""
    try:
        df = get_sheet_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки таблицы: {str(e)}")
    
    group_col = find_group_column(df, group)
    
    if group_col is None:
        raise HTTPException(status_code=404, detail=f"Группа '{group}' не найдена")
    
    schedule = parse_schedule(df, group_col)
    
    return JSONResponse(content={
        "group": group,
        "schedule": schedule
    })


@app.get("/")
async def root():
    return {"message": "Schedule API. Use /schedule?group=GROUP_NAME"}
