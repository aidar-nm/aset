# app.py
import streamlit as st
import pandas as pd
import asyncio
from datetime import datetime

from db import init_db, load_all_lots
from export import export_to_excel_rus
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# ⚠️ ВАЖНО: в sync.py должны быть эти функции
from sync import run_incremental_parser, run_full_parser

# =============================
# Конфиг / версия
# =============================
__version__ = "0.4.0"

st.set_page_config(page_title=f"Парсер закупок med.ecc.kz v{__version__}", layout="wide")
st.title(f"📦 Парсер закупок med.ecc.kz v{__version__}")

# =============================
# Инициализация БД
# =============================
init_db()

# =============================
# Кэш загрузки всех данных
# =============================
@st.cache_data(ttl=300)
def get_data():
    df = load_all_lots()
    # Сортируем: свежие сверху
    df = df.sort_values(['date_end', 'ann_id'], ascending=[False, False])
    df = df.reset_index(drop=True)
    df.index += 1
    return df

# =============================
# Функция фильтрации данных
# =============================
def filter_data(df, keyword, min_sum, date_limit, statuses, ls_list=None):
    # Ключевое слово — в title/description
    if keyword:
        df = df[
            df["title"].str.contains(keyword, case=False, na=False) |
            df["description"].str.contains(keyword, case=False, na=False)
        ]
    # Минимальная сумма
    if min_sum and min_sum > 0:
        df = df[df["amount"] >= min_sum]
    # Ограничение по дате окончания
    if date_limit:
        df = df[pd.to_datetime(df["date_end"], errors="coerce") <= pd.to_datetime(date_limit)]
    # Статусы
    if statuses:
        # Если выбраны не все статусы — фильтруем
        all_statuses = set(df["status"].dropna().unique().tolist())
        if set(statuses) != all_statuses:
            df = df[df["status"].isin(statuses)]
    # Поиск по списку ЛС (частичное совпадение в title/description)
    if ls_list:
        ls_list = [s.strip().lower() for s in ls_list if s and s.strip()]
        if len(ls_list) > 0:
            mask = df["title"].str.lower().apply(
                lambda x: any(s in (x or "") for s in ls_list)
            ) | df["description"].str.lower().apply(
                lambda x: any(s in (x or "") for s in ls_list)
            )
            df = df[mask]
    return df

# =============================
# Панель управления БД (сайдбар)
# =============================
with st.sidebar:
    st.header("⚙️ Управление БД")

    # Для инкрементального режима можно ограничивать число просматриваемых страниц
    max_pages = st.number_input(
        "Макс. страниц для обновления (только новые)",
        min_value=1, max_value=5000, value=30, step=1
    )

    # Подтверждение полного обновления через session_state
    if "confirm_full" not in st.session_state:
        st.session_state.confirm_full = False

    if st.button("Полное обновление (собрать весь архив)"):
        st.session_state.confirm_full = True

    if st.session_state.confirm_full:
        st.warning("⚠️ Это удалит все записи и соберёт весь архив сайта. Продолжить?")
        col_ok, col_cancel = st.columns(2)
        with col_ok:
            if st.button("Да, выполнить"):
                with st.spinner("Идёт ПОЛНОЕ обновление (все страницы)..."):
                    # run_full_parser без параметров: идём до пустой страницы
                    new_lots, log_path = asyncio.run(run_full_parser())
                    st.success(f"✅ Полное обновление завершено. Записано лотов: {len(new_lots)}")
                    st.info(f"Лог: {log_path}")
                st.cache_data.clear()
                st.session_state.confirm_full = False
        with col_cancel:
            if st.button("Отмена"):
                st.session_state.confirm_full = False

    if st.button("Обновить БД (только новые)"):
        with st.spinner("Идёт обновление базы (только новые)..."):
            # Функция должна вернуть (new_lots, log_path)
            new_lots, log_path = asyncio.run(run_incremental_parser(max_pages))
            st.success(f"✅ Добавлено новых лотов: {len(new_lots)}")
            st.info(f"Лог: {log_path}")
        st.cache_data.clear()

    last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.info(f"🕒 Последнее обновление: {last_update}")

# =============================
# Фильтры (сайдбар)
# =============================
st.sidebar.header("🔍 Фильтрация данных")
keyword = st.sidebar.text_input("Ключевое слово")
min_sum = st.sidebar.number_input("Мин. сумма", min_value=0, value=0)
date_limit = st.sidebar.date_input("До даты окончания", value=None)

# Поиск по списку ЛС из .txt
st.sidebar.header("🔬 Поиск по списку ЛС")
ls_file = st.sidebar.file_uploader("Загрузите .txt файл со списком ЛС", type="txt")
ls_list = []
if ls_file is not None:
    ls_content = ls_file.read().decode("utf-8")
    ls_list = [line.strip() for line in ls_content.splitlines() if line.strip()]
    st.sidebar.success(f"Загружено ЛС: {len(ls_list)}")

# =============================
# Загрузка и фильтрация данных
# =============================
data = get_data()
statuses_all = sorted(list(data["status"].dropna().unique()))
selected_statuses = st.sidebar.multiselect(
    "Статус объявления",
    options=statuses_all,
    default=statuses_all
)

filtered_data = filter_data(data, keyword, min_sum, date_limit, selected_statuses, ls_list)

# =============================
# Подготовка вывода (русские заголовки)
# =============================
cols_display = ["ann_id", "customer", "plan_point_id", "title", "description", "quantity", "amount", "status"]
display_df = filtered_data[cols_display].copy()
display_df.columns = [
    "Номер объявления",
    "Организатор",
    "Номер пункта плана",
    "Наименование лота",
    "Краткая характеристика",
    "Кол-во",
    "Сумма",
    "Статус",
]
display_df.index.name = "№"

# Быстрый поиск по всем видимым столбцам (опционально)
search_query = st.text_input("🔍 Быстрый поиск по всем столбцам")
df_filtered = display_df
if search_query:
    sq = search_query.lower()
    df_filtered = df_filtered[df_filtered.apply(
        lambda row: row.astype(str).str.lower().str.contains(sq).any(), axis=1
    )]

# =============================
# Лимит отображения (важно для Streamlit Cloud 200MB)
# =============================
MAX_ROWS_TO_SHOW = 50000  # можно уменьшить до 10–20k при очень больших наборах
rows_total = len(df_filtered)
if rows_total > MAX_ROWS_TO_SHOW:
    st.warning(
        f"Найдено {rows_total} записей. Для стабильной работы показываю первые {MAX_ROWS_TO_SHOW}. "
        f"Выгрузить всё целиком — через экспорт ниже."
    )
    df_to_show = df_filtered.head(MAX_ROWS_TO_SHOW)
else:
    df_to_show = df_filtered

# =============================
# Таблица (AgGrid)
# =============================
st.subheader(f"🗃️ Результаты ({rows_total} записей)")
gb = GridOptionsBuilder.from_dataframe(df_to_show)
gb.configure_default_column(wrapText=True, autoHeight=True)
gb.configure_selection("single", use_checkbox=True)
gb.configure_grid_options(domLayout='normal')
grid_options = gb.build()

grid_response = AgGrid(
    df_to_show,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    allow_unsafe_jscode=True,
    fit_columns_on_grid_load=True,
    height=420,
)

# =============================
# Детали выбранного лота (expander)
# =============================
selected_rows = grid_response['selected_rows']
if selected_rows is not None and len(selected_rows) > 0:
    # В твоей связке AgGrid => DataFrame
    selected_row = selected_rows.iloc[0]

    # Берём уникальные поля из видимых колонок
    ann_id_val = selected_row['Номер объявления']
    plan_point_id_val = selected_row['Номер пункта плана']

    # Ищем строку в ОРИГИНАЛЬНОМ filtered_data по тех. полям
    details_df = filtered_data[
        (filtered_data["ann_id"] == ann_id_val) &
        (filtered_data["plan_point_id"] == plan_point_id_val)
    ]
    if not details_df.empty:
        row = details_df.iloc[0]

        # Русские имена и порядок полей (первым — Статус)
        field_names = {
            "status": "Статус",
            "title": "Наименование лота",
            "description": "Краткая характеристика",
            "customer": "Организатор",
            "item_type": "Вид предмета",
            "unit": "Единица измерения",
            "quantity": "Кол-во",
            "price": "Цена за ед.",
            "amount": "Сумма",
            "date_start": "Дата начала",
            "date_end": "Дата окончания",
            "method": "Способ закупки",
            "plan_point_id": "Номер пункта плана",
            "lot_id": "ID лота",
            "ann_id": "Номер объявления",
        }
        fields_order = [
            "status",
            "title", "description", "customer",
            "item_type", "unit", "quantity", "price", "amount",
            "date_start", "date_end", "method",
            "plan_point_id", "lot_id", "ann_id",
        ]

        st.subheader("📌 Детали выбранного лота")
        with st.expander("Показать все поля", expanded=True):
            for key in fields_order:
                if key in row.index and pd.notna(row[key]):
                    st.write(f"**{field_names.get(key, key)}:** {row[key]}")
    else:
        st.warning("Не удалось найти подробности по выбранному лоту.")

# =============================
# Экспорт
# =============================
st.subheader("📊 Экспорт данных")
col1, col2 = st.columns(2)
with col1:
    if st.button("📥 Скачать Excel (все отфильтрованные)"):
        excel_file = export_to_excel_rus()
        with open(excel_file, "rb") as file:
            st.download_button(
                label="⬇️ Скачать Excel",
                data=file,
                file_name=excel_file,
                mime="application/vnd.ms-excel"
            )
with col2:
    if st.button("📥 Скачать CSV (все отфильтрованные)"):
        csv_data = filtered_data.to_csv(index=False)
        st.download_button(
            label="⬇️ Скачать CSV",
            data=csv_data,
            file_name="zakupki.csv",
            mime="text/csv"
        )
