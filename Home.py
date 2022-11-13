import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, TypedDict
from urllib import parse
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import tzdata
import streamlit as st


def download_and_unzip(
    url: str,
    extract_to: Optional[str] = None,
    encode: Optional[str] = None,
    decode: Optional[str] = None,
) -> list[str]:

    if extract_to is None:
        extract_to = "./"
    else:
        Path(extract_to).mkdir(parents=True, exist_ok=True)

    encode = "cp437" if encode is None else encode
    decode = "big5" if decode is None else decode

    with tempfile.TemporaryFile() as tf:

        print("Downloading started")
        tf.write(requests.get(url).content)
        print("Downloading Completed")

        extracted_files: list[str] = []
        with ZipFile(tf, "r") as zf:
            for file in zf.namelist():
                zf.extract(file, extract_to)
                file_path: Path = Path(extract_to).joinpath(file)

                if encode and decode:
                    decoded_file: str = file.encode(encode).decode(decode)
                    decoded_file_path: Path = Path(extract_to).joinpath(decoded_file)
                    file_path.replace(decoded_file_path)
                    file_path = decoded_file_path

                extracted_files.append(str(file_path))
                print(f"extracted: {file_path}")

    return extracted_files


def rename_file_w_datetime(
    file_path: str,
    time_zone: Optional[str] = None,
    datetime_format: Optional[str] = None,
    dry_run: bool = False,
) -> str:

    time_zone = "Asia/Taipei" if time_zone is None else time_zone
    datetime_format = "%Y%m$d" if datetime_format is None else datetime_format

    datetime_now = datetime.now(tz=ZoneInfo(time_zone)).strftime(datetime_format)
    file_split: list[str] = file_path.rsplit(".", 1)
    new_file_path: str = f"{file_split[0]}_{datetime_now}.{file_split[1]}"

    if not dry_run:
        Path(file_path).replace(new_file_path)
        print(f"rename '{file_path}' to '{new_file_path}'")

    return new_file_path


def fetch_bod_dataset(
    extract_to: Optional[str] = None,
    time_zone: Optional[str] = None,
    datetime_format: Optional[str] = None,
) -> str:

    # Source: 政府資料開放平臺 - 董監事資料集 (每月底更新)
    # https://data.gov.tw/dataset/96731

    extract_to = "./董監事資料集" if extract_to is None else extract_to
    datetime_format = "%Y%m" if datetime_format is None else datetime_format

    bod_dataset: str = str(Path(extract_to).joinpath("董監事資料集.csv"))
    bod_dataset = rename_file_w_datetime(
        bod_dataset, time_zone=time_zone, datetime_format=datetime_format, dry_run=True
    )

    if Path(bod_dataset).exists():
        print(f"{bod_dataset} exists")

    else:
        dataset_url = (
            "https://data.gcis.nat.gov.tw/od/file"
            "?oid=7E5201D9-CAD2-494E-8920-5319D66F66A1"
        )

        bod_dataset = download_and_unzip(dataset_url, extract_to=extract_to)[0]
        bod_dataset = rename_file_w_datetime(
            bod_dataset, time_zone=time_zone, datetime_format=datetime_format
        )

    return bod_dataset


def search_bod_dataset(
    extract_to: Optional[str] = None,
    time_zone: Optional[str] = None,
    datetime_format: Optional[str] = None,
    business_no: Optional[list[str]] = None,
    company_name: Optional[list[str]] = None,
    bod_directors: Optional[list[str]] = None,
    representative_for: Optional[list[str]] = None,
    union: Optional[bool] = None,
) -> pd.DataFrame:

    # 依統一編號、公司名稱、姓名、所代表法人搜索董監事資料集

    union = True if union is None else union

    bod_dataset = fetch_bod_dataset(
        extract_to=extract_to, time_zone=time_zone, datetime_format=datetime_format
    )

    companies: pd.DataFrame = pd.read_csv(
        bod_dataset,
        dtype=str,
    ).dropna(subset=["統一編號"])

    cols: dict[str, Optional[list[str]]] = {}
    if business_no is not None:
        cols["統一編號"] = business_no
    if company_name is not None:
        cols["公司名稱"] = company_name
    if bod_directors is not None:
        cols["姓名"] = bod_directors
    if representative_for is not None:
        cols["所代表法人"] = representative_for

    if len(cols) > 0:
        con: pd.DataFrame = companies[cols.keys()].isin(cols)
        con = con.any(axis=1) if union else con.all(axis=1)
        companies = companies[con].reset_index(drop=True)

    return companies


class GCISQuery(TypedDict, total=False):
    business_no: str
    company_name: str
    company_status: Optional[str]
    skip: Optional[int]
    top: Optional[int]


def search_gcis_dataset(
    dataset_url: str,
    query: GCISQuery,
    skip: Optional[int] = None,
    top: Optional[int] = None,
) -> Optional[pd.DataFrame]:

    # 從第n筆開始條列, 下限為0，上限為500000
    if (skip is None) or (skip < 0):
        query["skip"] = 0
    elif skip > 500000:
        query["skip"] = 500000
    else:
        query["skip"] = skip

    # 每次可撈取n筆, 下限為1，上限為1000
    if (top is None) or (top > 1000):
        query["top"] = 1000
    elif top <= 0:
        query["top"] = 1
    else:
        query["top"] = top

    try:
        df_list: list[pd.DataFrame] = []
        search_more: bool = True
        while search_more:
            company: pd.DataFrame = pd.read_json(
                dataset_url.format(**query), dtype=False
            )
            df_list.append(company)

            if len(company) == 1000:
                query["skip"] += 1000  # type: ignore
            else:
                search_more = False
                company = pd.concat(df_list, ignore_index=True)

        return company

    except:
        return None


def search_company_by_business_no(
    business_no: str,
    skip: Optional[int] = None,
    top: Optional[int] = None,
) -> Optional[pd.DataFrame]:

    # 以公司統一編號抓取公司基本資料
    # Source: 商工行政資料開放平臺 - 公司登記基本資料-應用一 (每小時更新)
    # https://data.gcis.nat.gov.tw/od/detail?oid=8776818F-EB3C-445F-BE95-AE22577CBEBC

    dataset_url: str = (
        "https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6"
        "?$format=json&$filter="
        "Business_Accounting_NO%20eq%20{business_no}&$"
        "skip={skip}&$"
        "top={top}"
    )

    query: GCISQuery = {"business_no": business_no}

    company: pd.DataFrame = search_gcis_dataset(dataset_url, query, skip=skip, top=top)

    return company


def search_company_by_name(
    company_name: str,
    company_status: Optional[str] = None,
    skip: Optional[int] = None,
    top: Optional[int] = None,
) -> Optional[pd.DataFrame]:

    # 以關鍵字抓取公司基本資料
    # Source: 商工行政資料開放平臺 - 公司登記關鍵字查詢 (每小時更新)
    # https://data.gcis.nat.gov.tw/od/detail?oid=311892F7-9BA6-4DDB-B5F6-AE1EF24FDD6A

    # 01為核准設立
    company_status = "01" if company_status is None else company_status

    dataset_url = (
        "https://data.gcis.nat.gov.tw/od/data/api/6BBA2268-1367-4B42-9CCA-BC17499EBE8C"
        "?$format=json&$filter="
        "Company_Name%20like%20{company_name}%20and%20"
        "Company_Status%20eq%20{company_status}&$"
        "skip={skip}&$"
        "top={top}"
    )

    query: GCISQuery = {
        "company_name": parse.quote(company_name),
        "company_status": company_status,
    }

    company: pd.DataFrame = search_gcis_dataset(dataset_url, query, skip=skip, top=top)

    return company


def search_bod_by_business_no(
    business_no: str,
    skip: Optional[int] = None,
    top: Optional[int] = None,
) -> Optional[pd.DataFrame]:

    # 以公司統一編號抓取公司董監事資料
    # Source: 商工行政資料開放平臺 - 公司登記董監事資料 (每小時更新)
    # https://data.gcis.nat.gov.tw/od/detail?oid=7CD44707-5D43-4C93-BC45-50E22F67EB01

    dataset_url = (
        "https://data.gcis.nat.gov.tw/od/data/api/4E5F7653-1B91-4DDC-99D5-468530FAE396"
        "?$format=json&$filter="
        "Business_Accounting_NO%20eq%20{business_no}&$"
        "skip={skip}&$"
        "top={top}"
    )
    query: GCISQuery = {"business_no": business_no}

    company: pd.DataFrame = search_gcis_dataset(dataset_url, query, skip=skip, top=top)

    return company


@st.cache
def convert_df(df: pd.DataFrame) -> Any:
    return df.to_csv().encode("utf-8-sig")


if __name__ == "__main__":
    st.set_page_config(page_title="台灣公司登記資料查詢", layout="wide")

    st.markdown(
        """
        # 台灣公司登記資料查詢
        ## 資料項目
        ### 台灣董監事資料集
        - 資料來源：政府資料開放平臺 (每月底更新)
        - URL: https://data.gov.tw/dataset/96731

        ### 公司登記關鍵字查詢
        - 資料來源：商工行政資料開放平臺 (每小時更新)
        - URL: https://data.gcis.nat.gov.tw/od/detail?oid=311892F7-9BA6-4DDB-B5F6-AE1EF24FDD6A

        ### 公司登記基本資料
        - 資料來源：商工行政資料開放平臺 (每小時更新)
        - URL: https://data.gcis.nat.gov.tw/od/detail?oid=8776818F-EB3C-445F-BE95-AE22577CBEBC

        ### 公司登記董監事資料
        - 資料來源：商工行政資料開放平臺 (每小時更新)
        - URL: https://data.gcis.nat.gov.tw/od/detail?oid=7CD44707-5D43-4C93-BC45-50E22F67EB01

        ## Source code
        https://github.com/weiyan-chen/TW_GCIS_Query
        """
    )
