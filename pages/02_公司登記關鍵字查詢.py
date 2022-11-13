import streamlit as st
import pandas as pd
from Home import search_company_by_name, convert_df

st.title("公司登記關鍵字查詢")
st.write("輸入多項條件時請以逗號 , 分隔")

form = st.form("search_company_by_name")

company_name_input: str = form.text_input("公司名稱關鍵字")
company_name = [item.strip() for item in company_name_input.split(",")]
submitted = form.form_submit_button("查詢")

input_is_empty = (company_name[0] == "") and (len(company_name) == 1)
if (input_is_empty is False) and (submitted):
    with st.spinner("查詢中..."):
        company_df: list[pd.DataFrame] = []

        for n in company_name:
            company = search_company_by_name(company_name=n)
            if company is not None:
                company_df.append(company)

        if len(company_df) > 0:
            company = pd.concat(company_df).drop_duplicates(ignore_index=True)
            st.dataframe(company, width=1080)

            csv = convert_df(company)
            st.download_button(
                label="下載為CSV檔",
                data=csv,
                file_name="company.csv",
                mime="text/csv",
            )

        else:
            st.write("未有符合條件之查詢結果")
