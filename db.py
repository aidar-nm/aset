import sqlite3
import os
import pandas as pd

DB_PATH = "data/medecc.db"
os.makedirs("data", exist_ok=True)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS lots (
        plan_point_id TEXT,    -- № пункта плана (уникальный номер лота)
        lot_id TEXT PRIMARY KEY,
        ann_id TEXT,
        title TEXT,
        customer TEXT,
        description TEXT,
        item_type TEXT,
        unit TEXT,
        quantity REAL,
        price REAL,
        amount REAL,
        date_start TEXT,
        date_end TEXT,
        method TEXT,
        status TEXT
    );
    """)
    conn.commit()
    conn.close()

def insert_lot(lot: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO lots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tuple(lot.values()))
    conn.commit()
    conn.close()

def lot_exists(lot_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM lots WHERE lot_id = ?", (lot_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def load_all_lots() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM lots ORDER BY ann_id, plan_point_id", conn)
    conn.close()
    return df

def get_last_update_date():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT MAX(date_end) FROM lots")
    result = c.fetchone()
    conn.close()
    return result[0] if result else None
