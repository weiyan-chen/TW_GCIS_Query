import streamlit as st
import pandas as pd
from Home import search_bod_by_business_no, convert_df

st.title("公司登記董監事資料")
st.write("輸入多項條件時請以逗號 , 分隔")

form = st.form("search_bod_by_business_no")

business_no_input: str = form.text_input("公司統一編號")
business_no = [item.strip() for item in business_no_input.split(",")]
submitted = form.form_submit_button("查詢")

input_is_empty = (business_no[0] == "") and (len(business_no) == 1)
if (input_is_empty is False) and (submitted):
    with st.spinner("查詢中..."):
        company_df: list[pd.DataFrame] = []

        for n in business_no:
            company = search_bod_by_business_no(business_no=n)
            if company is not None:
                company.insert(0, "Business_Accounting_NO", str(n))
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
