import streamlit as st
import pandas as pd
import asyncio
from db import init_db, load_all_lots
from sync import run_parser
from export import export_to_excel_rus
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import streamlit as st

# Инициализация базы данных
init_db()

# Загрузка и кэширование данных
@st.cache_data(ttl=300)
def get_data():
    df = load_all_lots()
    df = df.reset_index(drop=True)
    df.index += 1
    return df

# Фильтрация данных
def filter_data(df, keyword, min_sum, date_limit):
    if keyword:
        df = df[df["title"].str.contains(keyword, case=False, na=False) |
                df["description"].str.contains(keyword, case=False, na=False)]
    if min_sum > 0:
        df = df[df["amount"] >= min_sum]
    if date_limit:
        df = df[pd.to_datetime(df["date_end"], errors="coerce") <= pd.to_datetime(date_limit)]
    return df

# Основное приложение Streamlit
st.set_page_config(page_title="Парсер закупок med.ecc.kz", layout="wide")
st.title("📦 Әсеттің көмекшісі! v0.2-04.08.2025")

# Панель управления парсингом
with st.sidebar:
    st.header("⚙️ Управление парсингом")
    pages = st.slider("Количество страниц для парсинга", 1, 30, 2)
    if st.button("🚀 Запустить парсинг"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        with st.spinner("Идёт парсинг данных..."):
            asyncio.run(run_parser(pages))   # <-- Вызов только ОДИН раз!
            progress_bar.progress(1.0)
            status_text.text(f"Парсинг {pages} страниц завершён.")
            st.success("✅ Парсинг завершён!")
            st.cache_data.clear()


    last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.info(f"🕒 Последнее обновление: {last_update}")

# Панель фильтрации данных
st.sidebar.header("🔍 Фильтрация данных")
keyword = st.sidebar.text_input("Ключевое слово")
min_sum = st.sidebar.number_input("Мин. сумма", min_value=0, value=0)
date_limit = st.sidebar.date_input("До даты окончания", value=None)

# Загрузка и фильтрация данных
data = get_data()
filtered_data = filter_data(data, keyword, min_sum, date_limit)

# Подготовка данных для отображения
cols_display = ["ann_id", "customer", "plan_point_id", "title", "description", "quantity", "amount"]
display_df = filtered_data[cols_display].copy()
display_df.columns = ["Номер объявления", "Организатор", "Номер лота", "Наименование лота", "Краткая характеристика", "Кол-во", "Сумма"]
display_df.index.name = "№"

search_query = st.text_input("🔍 Быстрый поиск по всем столбцам")

# Настройка таблицы AgGrid
gb = GridOptionsBuilder.from_dataframe(display_df)
gb.configure_default_column(wrapText=True, autoHeight=True)
gb.configure_selection("single", use_checkbox=True)
gb.configure_grid_options(domLayout='normal')
grid_options = gb.build()

# Отображение данных с возможностью поиска и выбора строки
df_filtered = display_df
if search_query:
    search_query_lower = search_query.lower()
    df_filtered = df_filtered[df_filtered.apply(
        lambda row: row.astype(str).str.lower().str.contains(search_query_lower).any(), axis=1
    )]

# Таблица
st.subheader(f"🗃️ Результаты ({len(df_filtered)} записей)")
grid_response = AgGrid(
    df_filtered,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    allow_unsafe_jscode=True,
    fit_columns_on_grid_load=True,
    height=400,
)

selected_rows = grid_response['selected_rows']

# Всплывающее окно с подробностями при выборе строки
#if selected_rows:
#    st.subheader("📌 Детали выбранного лота")
#    selected_row = selected_rows[0]
#    selected_index = selected_row["_selectedRowNodeInfo"]["nodeRowIndex"]
#    details = filtered_data.iloc[selected_index]
#    st.json(details.to_dict())

# Экспорт данных
st.subheader("📊 Экспорт данных")
col1, col2 = st.columns(2)
with col1:
    if st.button("📥 Скачать Excel"):
        excel_file = export_to_excel_rus()
        with open(excel_file, "rb") as file:
            st.download_button(
                label="⬇️ Скачать Excel",
                data=file,
                file_name=excel_file,
                mime="application/vnd.ms-excel"
            )
with col2:
    if st.button("📥 Скачать CSV"):
        csv_data = filtered_data.to_csv(index=False)
        st.download_button(
            label="⬇️ Скачать CSV",
            data=csv_data,
            file_name="zakupki.csv",
            mime="text/csv"
        )





