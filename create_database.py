import sqlite3

conn = sqlite3.connect("mydatabase.db")  # или :memory: чтобы сохранить в RAM
cursor = conn.cursor()

# Создание таблицы
cursor.execute("""CREATE TABLE citizens
                  (import_id INTEGER, citizen_id INTEGER, town text,
                   street text, building text, appartement INTEGER, 
                   name text, birth_date text, gender text)
               """)

cursor.execute("""CREATE TABLE relatives
                  (import_id INTEGER, citizen_id INTEGER, relative_id INTEGER)
               """)
