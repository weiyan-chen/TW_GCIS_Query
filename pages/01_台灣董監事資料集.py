from typing import Optional
import streamlit as st
from Home import search_bod_dataset, convert_df

st.title("台灣董監事資料集")
st.write("輸入多項條件時請以逗號 , 分隔")

form = st.form("search_bod_dataset")

business_no_input: str = form.text_input("公司統一編號")
business_no: Optional[list[str]]
if business_no_input == "":
    business_no = None
else:
    business_no = [item.strip() for item in business_no_input.split(",")]

company_name_input: str = form.text_input("公司登記名稱")
company_name: Optional[list[str]]
if company_name_input == "":
    company_name = None
else:
    company_name = [item.strip() for item in company_name_input.split(",")]

bod_directors_input: str = form.text_input("公司董事會成員性名")
bod_directors: Optional[list[str]]
if bod_directors_input == "":
    bod_directors = None
else:
    bod_directors = [item.strip() for item in bod_directors_input.split(",")]

representative_for_input: str = form.text_input("董事會成員所代表法人")
representative_for: Optional[list[str]]
if representative_for_input == "":
    representative_for = None
else:
    representative_for = [item.strip() for item in representative_for_input.split(",")]

union: Optional[bool]
union_for_input = form.selectbox("篩選模式", ("聯集", "交集"), index=0)
if union_for_input == "聯集":
    union = True
else:
    union = False

input_is_empty = True
for i in [business_no, company_name, bod_directors, representative_for]:
    if i is not None:
        input_is_empty = False
        break

submitted = form.form_submit_button("查詢")
if (input_is_empty is False) and (submitted):
    with st.spinner("查詢中..."):
        bod_dataset = search_bod_dataset(
            extract_to="./董監事資料集",
            business_no=business_no,
            company_name=company_name,
            bod_directors=bod_directors,
            representative_for=representative_for,
            union=union,
        )
        if bod_dataset.empty is False:

            st.dataframe(bod_dataset, width=1080)

            csv = convert_df(bod_dataset)
            st.download_button(
                label="下載為CSV檔",
                data=csv,
                file_name="bod_dataset.csv",
                mime="text/csv",
            )
        else:
            st.write("未有符合條件之查詢結果")
