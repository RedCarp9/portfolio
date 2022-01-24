import os,sys,io,math,time,hashlib,datetime,matplotlib,japanize_matplotlib,re,sqlite3,hashlib
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import RendererAgg
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
# Google用オプション: 
from selenium.webdriver.chrome.options import Options as G_Options
# FireFox用オプション
from selenium.webdriver.firefox.options import Options as F_Options

# =============================================================================
# ボタンの書式等設定
# =============================================================================
button_css = f"""
<style>
  div.stButton > button:first-child  {{               */
    border       : 10px solid #fff     ;/* 枠線：黒色で10ピクセルの実線 */
    border-radius: 10px 10px 10px 10px ;/* 枠線：半径10ピクセルの角丸     */
    background   : #ddd                ;/* 背景色：薄いグレー            */
  }}
</style>
"""
# button_css = f"""
# <style>
#   div.stButton > button:first-child  {{
#     font-weight  : bold                ;/* 文字：太字                   */
#     border       :  5px solid #f36     ;/* 枠線：ピンク色で5ピクセルの実線 */
#     border-radius: 10px 10px 10px 10px ;/* 枠線：半径10ピクセルの角丸     */
#     background   : #ddd                ;/* 背景色：薄いグレー            */
#   }}
# </style>
# """

# =============================================================================
# メインプログラム
# =============================================================================
def main():   
    # データベース名’MyProperty.db’を準備
    conn = sqlite3.connect('MyProperty.db')
    # カーソルオブジェクトの作成
    cur = conn.cursor()
    # ページ全体に表示するように設定
    st.set_page_config(layout="wide")
    # ボタンの書式設定
    st.markdown(button_css, unsafe_allow_html=True)
    
    if not table_isexist(conn, cur, 'nikkei_data') or not table_isexist(conn, cur, 'stock_data'):
        st.header('初期データの読込1')
        if not table_isexist(conn, cur, 'nikkei_data'):
            nikkei_file = st.file_uploader('日経銘柄データファイル（CSV）をアップロードしてください', type='csv')
            st.info('Excelデータのダウンロード：https://www.jpx.co.jp/markets/statistics-equities/misc/01.html')
    
        if not table_isexist(conn, cur, 'stock_data'):
            stock_upload_file = st.file_uploader('国内株データのファイルをアップロードしてください', type='csv')
            st.info('csvデータのダウンロード：http://kabudata-dll.com/')
        
        if st.button("読み込み開始"):
            placeholder = st.empty()
            placeholder.info('日経銘柄データ読込開始')
            df_j = pd.read_csv(nikkei_file)
            df_j = df_j.astype({'コード':str})
            # IR BANKのURLを追加
            df_j['IR_BANK'] = "https://irbank.net/"+df_j['コード']
            df_j = df_j.rename(columns={'17業種区分':'industry17','33業種区分':'industry33'})
            # sqlにnikkei_dataテーブルを追加もしくは置換
            df_j.to_sql('nikkei_data', conn, if_exists='replace',index=None)
            
            # カラーリストの取得
            color_list = create_color_list()
            color_list = create_color_list(color_list=color_list,color_index=len(color_list),max_num=225,min_num=25,d_num=100)
            # データベースから'industry17'をグループ化で抜き出し、重複を削除。その後ソート
            query = """
                select industry17 from nikkei_data
                group by industry17
                having industry17 != '-'
            """
            df_color = pd.read_sql_query(sql = query, con = conn)
            df_color = df_color.sort_values(['industry17'])
            df_color['industry17_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['industry17']))]
            # sqlにcolor_industry17テーブルを追加もしくは置換
            df_color.to_sql('color_industry17', conn, if_exists='replace',index=None)
            
            # データベースから'industry33'をグループ化で抜き出し、重複を削除。その後ソート
            query = """
                select industry33 from nikkei_data
                group by industry33
                having industry33 != '-'
            """
            df_color = pd.read_sql_query(sql = query, con = conn)
            df_color = df_color.sort_values(['industry33'])
            df_color['industry33_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['industry33']))]
            # sqlにcolor_industry17テーブルを追加もしくは置換
            df_color.to_sql('color_industry33', conn, if_exists='replace',index=None)
            
            placeholder.info('株価データ読込開始')
            df_stock = pd.read_csv(stock_upload_file, encoding = "shift-jis")
            df_stock = df_stock.astype({'銘柄コード':str})
            # df_stock = df_stock.astype({'銘柄コード':str, '始値':float, '高値':float, '安値':float, '終値':float, '配当':float, 'PER':float, 'PBR':float})
            # df_stock['現在日付'] = pd.to_datetime(df_stock['現在日付'])
            # df_stock['配当落日'] = pd.to_datetime(df_stock['配当落日'])
            
            # sqlにnikkei_dataテーブルを追加もしくは置換
            df_stock.to_sql('stock_data', conn, if_exists='replace',index=None)
            placeholder.info('株価データの読込終了')
            
            placeholder.info('設定中')
            if not table_isexist(conn, cur, 'portfolio_data_column'):
               df_original_column = {'original':['コード','銘柄名','市場・商品区分','カテゴリー','証券会社','17業種区分','33業種区分','保有数量(株)','平均取得単価(円)','評価額(円)','取得総額(円)','損益(円)','損益(%)','預り区分'],
                                       'changed':['code','brand','market','category','securities_account','industry17','industry33','posession_num','average_acquisition_yen','valuation_yen','total_acquisition_yen','PL_yen','PL_per','classification']}
               pd.DataFrame(df_original_column).to_sql('portfolio_data_column', conn, if_exists='replace',index=None)
            placeholder.info('F5キーで更新してください')
    
    else:
        # モード種類と関数名の設定
        modes = {"データ取得・更新":data_input,
              "保有資産表示":my_portfolio,
              "理想のポートフォリオ（国内株）表示・編集":ideal_portfolio,
              "国内株銘柄検索（業種別）・登録":search_domestic_stock}
        
        # モード選択用セレクトボックスの作成
        select_mode_name = st.sidebar.selectbox("モード選択",tuple(modes.keys()))
        # セレクトボックスで選択された関数を実行
        mode_func = modes[select_mode_name]
        mode_func(conn, cur)
        
        # データベースを閉じる
        cur.close()
        conn.close()


# =============================================================================
# モード：データ入力の処理
# =============================================================================
def data_input(conn, cur):
    st.header("データ取得・更新")
    st.subheader('証券口座情報の読込・更新')
    # ドライバー選択画面をサイドバーに追加
    select_driver = st.sidebar.selectbox("ドライバー選択",["Google","FireFox"])
    
    ## チェックボックスを表示し、各チェックボックスが選択されたときに、それぞれの処理を実行
    sbi_check = st.checkbox('SBI証券')
    if sbi_check:
        sbi_select_dict = {'ネットから取得':input_sbi,'CSVファイルから取得':input_sbi_csv}
        sbi_select = st.selectbox('データ取得方法の選択',tuple(sbi_select_dict.keys()))
        sbi_select_func = sbi_select_dict[sbi_select]
        sbi_select_func()
    # rakuten_check = st.checkbox('楽天証券')
    # if rakuten_check: input_rakuten()
    neo_check = st.checkbox('SBIネオモバイル証券')
    if neo_check: input_neo()
    
    # 日経銘柄情報がデータベース上になければ、ファイルをアップロードしてデータを取得
    st.subheader('日経銘柄データの登録・更新')
    if table_isexist(conn, cur, 'nikkei_data'):
        nikkei_check = st.checkbox('日経銘柄情報の更新')
        if nikkei_check:
            nikkei_upload_file = st.file_uploader("日経銘柄データファイル（CSV）", type='csv')
            st.info('Excelデータのダウンロード：https://www.jpx.co.jp/markets/statistics-equities/misc/01.html')
    else:
        nikkei_check = True
        nikkei_upload_file = st.file_uploader("日経銘柄データファイル（CSV）", type='csv')
        st.info('日経銘柄Excelデータのダウンロード：https://www.jpx.co.jp/markets/statistics-equities/misc/01.html')
        
    # 配当金情報がデータベース上になければ、ファイルをアップロードしてデータを取得
    st.subheader('株価データ（国内）の登録・更新')
    if table_isexist(conn, cur, 'stock_data'):
        stock_check = st.checkbox('株価情報の更新')
        if stock_check:
            stock_upload_file = st.file_uploader("株価データファイル（CSV）", type='csv')
            st.info('csvデータのダウンロード：http://kabudata-dll.com/')
    else:
        stock_check = True
        stock_upload_file = st.file_uploader("株価データファイル（CSV）", type='csv')
        st.info('csvデータのダウンロード：http://kabudata-dll.com/')
    
    
    # "読み込み開始"ボタンが押されたときの処理
    if st.button("読み込み開始"):
        # 日経銘柄情報を読み込む必要があるのに、ファイルがアップロードされていない時にエラー文を表示
        if not table_isexist(conn, cur, 'nikkei_data') and not nikkei_upload_file:
            st.error('日経銘柄データのファイルをアップロードしてください')
            st.stop()
        elif nikkei_check and not nikkei_upload_file:
            st.error('日経銘柄データのファイルをアップロードしてください')
            st.stop()
            
        # 配当金情報を読み込む必要があるのに、ファイルがアップロードされていない時にエラー文を表示
        if not table_isexist(conn, cur, 'stock_data') and not stock_upload_file:
            st.error('株価データのファイルをアップロードしてください')
            st.stop()
        elif stock_check and not stock_upload_file:
            st.error('株価データのファイルをアップロードしてください')
            st.stop()
        
        # カラーリストの取得
        color_list = create_color_list()
        color_list = create_color_list(color_list=color_list,color_index=len(color_list),max_num=225,min_num=25,d_num=100)
        
        # プレースホルダーの設定
        placeholder = st.empty()
        # 日経銘柄情報がデータベース上に無い、もしくは、更新が選択された時、アップロードされたファイルからデータを取得
        if not table_isexist(conn, cur, 'nikkei_data') or nikkei_check:
            placeholder.info('日経銘柄データ読込開始')
            df_j = pd.read_csv(nikkei_upload_file)
            df_j = df_j.astype({'コード':str})
            # IR BANKのURLを追加
            df_j['IR_BANK'] = "https://irbank.net/"+df_j['コード']
            
            df_j = df_j.rename(columns={'17業種区分':'industry17','33業種区分':'industry33'})
            # sqlにnikkei_dataテーブルを追加もしくは置換
            df_j.to_sql('nikkei_data', conn, if_exists='replace',index=None)
            
            # データベースから'industry17'をグループ化で抜き出し、重複を削除。その後ソート
            query = """
                select industry17 from nikkei_data
                group by industry17
                having industry17 != '-'
            """
            df_color = pd.read_sql_query(sql = query, con = conn)
            df_color = df_color.sort_values(['industry17'])
            df_color['industry17_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['industry17']))]
                
            # sqlにcolor_industry17テーブルを追加もしくは置換
            df_color.to_sql('color_industry17', conn, if_exists='replace',index=None)
            
            # データベースから'industry33'をグループ化で抜き出し、重複を削除。その後ソート
            query = """
                select industry33 from nikkei_data
                group by industry33
                having industry33 != '-'
            """
            df_color = pd.read_sql_query(sql = query, con = conn)
            df_color = df_color.sort_values(['industry33'])
            df_color['industry33_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['industry33']))]
                
            # sqlにcolor_industry17テーブルを追加もしくは置換
            df_color.to_sql('color_industry33', conn, if_exists='replace',index=None)
            
            placeholder.info('日経銘柄データの読込終了')
            
        else:
            # sqlのにnikkei_dataテーブルから全データの取り出し
            df_j = pd.read_sql_query('SELECT * FROM nikkei_data', conn)
        
        # 証券口座が選択されていた場合、データの読み込みを行う
        if sbi_check or neo_check:
                 
            # 各ポートフォリオデータのキーをConcat_data_keyに格納するための準備
            Concat_data = {}
            # sbiが選択されたとき、ポートフォリオのデータを取得
            if sbi_check:
                placeholder.info('')
                # 選択された方法でデータの取得
                if sbi_select == 'ネットから取得':
                    placeholder.info('ドライバー起動中')
                    # ヘッドレスモードの設定とドライバーの使用
                    try:
                        if select_driver == "Google":
                            options = G_Options()
                            options.add_argument('--headless')
                            driver = webdriver.Chrome(options=options)
                            
                        elif select_driver == "FireFox":
                            options = F_Options()
                            options.add_argument('--headless')
                            driver = webdriver.Firefox(options=options)
                    except:
                        ## ドライバーが無いとき
                        placeholder.error("ドライバーを確認してください")
                        st.stop()
                    df_sbi = get_sbi_data(df_j,driver,placeholder)
                elif sbi_select == 'CSVファイルから取得': df_sbi = get_data_sbi_csv(df_j)
                
                df_sbi = df_sbi.astype({'評価額(円)':'float64','損益(円)':'float64','現在値(円)':'float64','保有数量(株)':'int64','損益(%)':'float64','平均取得単価(円)':'float64'})
                # sqlにsbi_dataテーブルを追加もしくは置換
                df_sbi.to_sql('sbi_data', conn, if_exists='replace',index=None)
                Concat_data['sbi'] = df_sbi
                placeholder.info('SBI証券データ読込終了')
                # export_data(st.session_state['sbi_data'],output_dir='\data',name='sbi_data')
            
            elif table_isexist(conn, cur, 'sbi_data'):
                placeholder.info('データベースからSBI証券データを読込中')
                Concat_data['sbi'] = pd.read_sql_query('SELECT * FROM sbi_data', conn)
                placeholder.info('SBI証券データ読込終了')
            
            # ネオモバイルが選択されたとき、ポートフォリオのデータを取得
            if neo_check:
                if (sbi_check and sbi_select!='ネットから取得') or not sbi_check:
                    placeholder.info('ドライバー起動中')
                    # ヘッドレスモードの設定とドライバーの使用
                    try:
                        if select_driver == "Google":
                            options = G_Options()
                            options.add_argument('--headless')
                            driver = webdriver.Chrome(options=options)
                            
                        elif select_driver == "FireFox":
                            options = F_Options()
                            options.add_argument('--headless')
                            driver = webdriver.Firefox(options=options)
                    except:
                        ## ドライバーが無いとき
                        placeholder.error("ドライバーを確認してください")
                        st.stop()
                placeholder.info('ネオモバイル証券データ読込開始')
                df_neo = get_neo_data(df_j,driver,placeholder)
                df_neo = df_neo.astype({'評価額(円)':'float64','損益(円)':'float64','現在値(円)':'float64','保有数量(株)':'int64','損益(%)':'float64','平均取得単価(円)':'float64'})
                # 取得総額(円)を計算
                df_neo['取得総額(円)'] = df_neo['平均取得単価(円)']*df_neo['保有数量(株)']
                
                df_neo.to_sql('neo_data', conn, if_exists='replace',index=None)
                Concat_data['neo'] = df_neo
                placeholder.info('ネオモバイル証券データ読込終了')
                # export_data(st.session_state['neo_data'],output_dir='\data',name='neo_data')
            
            elif table_isexist(conn, cur, 'neo_data'):
                placeholder.info('データベースからネオモバイル証券データを読込中')
                Concat_data['neo'] = pd.read_sql_query('SELECT * FROM neo_data', conn)
                placeholder.info('ネオモバイル証券データ読込終了')
            
            ## ウィンドウを閉じる
            if (sbi_check and sbi_select=='ネットから取得') or  neo_check:
                placeholder.info('ドライバー終了中')
                driver.quit()
            
            # portfolio_data表の列名設定
            # portfolio_data_columnデータベースにない場合、新規に作成
            if not table_isexist(conn, cur, 'portfolio_data_column'):
               df_original_column = {'original':['コード','銘柄名','市場・商品区分','カテゴリー','証券会社','17業種区分','33業種区分','保有数量(株)','平均取得単価(円)','評価額(円)','取得総額(円)','損益(円)','損益(%)','預り区分'],
                                       'changed':['code','brand','market','category','securities_account','industry17','industry33','posession_num','average_acquisition_yen','valuation_yen','total_acquisition_yen','PL_yen','PL_per','classification']}
               pd.DataFrame(df_original_column).to_sql('portfolio_data_column', conn, if_exists='replace',index=None)
                
            # データベースから読込
            df_column_dict = {}
            query = """
                select * from portfolio_data_column
            """
            for i in cur.execute(query): df_column_dict[i[0]] = i[1]

            placeholder.info('ポートフォリオデータ作成中')
            # 必要なデータをneed_dataに格納して、データフレームから取り出し
            need_data = ['コード','銘柄名','市場・商品区分','industry33','industry17','保有数量(株)','平均取得単価(円)','評価額(円)','取得総額(円)','損益(円)','損益(%)','カテゴリー','証券会社','預り区分']
            for i in Concat_data: Concat_data[i] = Concat_data[i].loc[:,need_data]
            # 最初に読み込んだデータを'portfolio_data'をキーとして記録
            df_portfolio = pd.DataFrame(index=[], columns=need_data)
            # キーが２つ以上ある時、結合処理を行う
            for i in Concat_data: df_portfolio = pd.concat([df_portfolio,Concat_data[i]],ignore_index=True)
            df_portfolio = df_portfolio.astype({'コード':str})
            df_portfolio = df_portfolio.rename(columns=df_column_dict)
            # sqlにportfolio_dataテーブルを追加もしくは置換
            df_portfolio.to_sql('portfolio_data', conn, if_exists='replace',index=None)
            # export_data(st.session_state['portfolio_data'],output_dir='\data',name='portfolio_data')
            
            
            # カテゴリーごとにカラー割り当て
            if not table_isexist(conn, cur, 'color_category'):
                df_color = pd.DataFrame({'category':['国内株','投資信託','米国株'],'category_color':["#ff4b4b","#4bff4b","#4b4bff"]})
                # sqlにcolor_categoryテーブルを追加もしくは置換
                df_color.to_sql('color_category', conn, if_exists='replace',index=None)
                
                   
            # 証券口座ごとにカラー割り当て
            if not table_isexist(conn, cur, 'color_securities_account'):
                df_color = pd.DataFrame({'securities_account':['SBI証券','楽天証券','ネオモバイル'],'securities_account_color':["#4b4bff","#ff4b4b","#4bff4b"]})
                # sqlにcolor_securities_accountテーブルを追加もしくは置換
                df_color.to_sql('color_securities_account', conn, if_exists='replace',index=None)
            
            # データベースから'銘柄名'の重複を削除したデータを抽出。その後ソートしてカラー情報を登録
            query = """
                select brand from portfolio_data
                group by brand
                order by brand
            """
            df_color = pd.read_sql_query(sql = query, con = conn)
            df_color = df_color.sort_values(['brand'],ascending=[True])
            df_color['brand_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['brand']))]  
            # sqlにcolor_brandテーブルを追加もしくは置換
            df_color.to_sql('color_brand', conn, if_exists='replace',index=None)
            
            # データベースから'預り区分'の重複を削除したデータを抽出。その後ソートしてカラー情報を登録
            query = """
                select classification from portfolio_data
                group by classification
                order by classification
            """
            df_color = pd.read_sql_query(sql = query, con = conn)
            # df_color = df_color.sort_values(['brand'],ascending=[True])
            df_color['color_classification'] = [color_list[i%len(color_list)] for i in range(len(df_color['classification']))]
            # sqlにcolor_classificationテーブルを追加もしくは置換
            df_color.to_sql('color_classification', conn, if_exists='replace',index=None)
            
            # 理想のポートフォリオデータの作成・変更
            # DBにideal_portfolioが無い場合、テーブルの作成
            if not table_isexist(conn, cur, 'ideal_portfolio'):
                query = """
                    select  my_data.code, my_data.brand, my_data.posession_num, my_data.valuation_yen, my_data.industry17, my_data.industry33,
                        (case stock_data.終値 when ' ' then '0.0001' else stock_data.終値 end) as 株価, (case stock_data.配当 when '-' then '0' else stock_data.配当 end) as 配当
                    from stock_data, (
                        select code, brand, sum(posession_num) as posession_num, sum(valuation_yen) as valuation_yen, industry17, industry33
                        from portfolio_data
                        where category=='国内株'
                        group by brand
                        order by brand) as my_data
                    where my_data.code == stock_data.銘柄コード
                    order by brand
                """
                df_ideal = pd.read_sql_query(sql = query, con = conn)
                df_ideal['税引前配当額'] = df_ideal['posession_num']*df_ideal['配当']
                df_ideal.to_sql('ideal_portfolio', conn, if_exists='replace',index=None)
                
                
                # # sqlにcolor_ideal_portfolioテーブルを追加もしくは置換
                # query = """
                #     select code,brand from ideal_portfolio
                #     group by brand
                #     order by brand
                # """
                # df_color = pd.read_sql_query(sql = query, con = conn)
                # df_color['brand_color'] = [color_list[i%len(color_list)] for i in range(len(df_ideal['brand']))]  
                # # sqlにcolor_ideal_portfolioテーブルを追加もしくは置換
                # df_color.to_sql('color_ideal_portfolio', conn, if_exists='replace',index=None)
                
            else:
                # ideal_portfolioに無いコードをportfolio_dataから取得し、ideal_portfolioに追加
                query = """
                    select my_data.code, my_data.brand, my_data.posession_num, my_data.valuation_yen, my_data.industry17, my_data.industry33,
                        (case stock_data.終値 when ' ' then '0.0001' else stock_data.終値 end) as 株価, (case stock_data.配当 when '-' then '0' else stock_data.配当 end) as 配当
                    from stock_data, (
                        select my_data1.code, my_data1.brand, my_data1.posession_num, my_data1.valuation_yen, my_data1.industry17, my_data1.industry33
                        from ideal_portfolio, (
                            select code, brand, sum(posession_num) as posession_num, sum(valuation_yen) as valuation_yen, industry17, industry33
                            from portfolio_data
                            where category=='国内株'
                            group by brand) as my_data1
                        where (my_data1.code not in (
                            select code
                            from ideal_portfolio))
                            or (ideal_portfolio.posession_num != my_data1.posession_num and ideal_portfolio.code == my_data1.code)) as my_data
                    where my_data.code == stock_data.銘柄コード
                    group by brand
                    order by brand
                """
                df_ideal = pd.read_sql_query(sql = query, con = conn)
                # 更新箇所がある場合のみ、DBの更新
                if len(df_ideal['code'])>0:
                    df_ideal['税引前配当額'] = df_ideal['posession_num']*df_ideal['配当']
                    query = """
                        select code
                        from ideal_portfolio
                    """
                    ideal_code_list = list(pd.read_sql_query(sql = query, con = conn)['code'])
                    for i in range(len(df_ideal['code'])):
                        if df_ideal['code'][i] in ideal_code_list:
                            query = f"""
                                update ideal_portfolio
                                set posession_num={df_ideal['posession_num'][i]}, valuation_yen={df_ideal['valuation_yen'][i]}, 税引前配当額={df_ideal['税引前配当額'][i]}
                                where code = {df_ideal['code'][i]}
                            """
                            # データ更新
                            cur.execute(query)
                        else:
                            query = """
                                insert into ideal_portfolio (code,brand,posession_num,valuation_yen,industry17,industry33, 株価, 配当, 税引前配当額)
                                values (?,?,?,?,?,?,?,?,?)
                            """
                            data = [df_ideal['code'][i], df_ideal['brand'][i], int(df_ideal['posession_num'][i]), df_ideal['valuation_yen'][i], df_ideal['industry17'][i], df_ideal['industry33'][i], df_ideal['株価'][i],  df_ideal['配当'][i], df_ideal['税引前配当額'][i]]
                            # データ注入
                            cur.execute(query,data)
                        # コミット
                        conn.commit()
                        
                # ideal_portfolioにデータがない場合、新規作成    
                elif len(df_ideal['code'])==0:
                    query = """
                        select  my_data.code, my_data.brand, my_data.posession_num, my_data.valuation_yen, my_data.industry17, my_data.industry33,
                            (case stock_data.終値 when ' ' then '0.0001' else stock_data.終値 end) as 株価, (case stock_data.配当 when '-' then '0' else stock_data.配当 end) as 配当
                        from stock_data, (
                            select code, brand, sum(posession_num) as posession_num, sum(valuation_yen) as valuation_yen, industry17, industry33
                            from portfolio_data
                            where category=='国内株'
                            group by brand
                            order by brand) as my_data
                        where my_data.code == stock_data.銘柄コード
                        order by brand
                    """
                    df_ideal = pd.read_sql_query(sql = query, con = conn)
                    df_ideal['税引前配当額'] = df_ideal['posession_num']*df_ideal['配当']
                    df_ideal.to_sql('ideal_portfolio', conn, if_exists='replace',index=None)
                
                # sqlにcolor_ideal_portfolioテーブルを追加もしくは置換
                query = """
                    select code, brand from ideal_portfolio
                    group by brand
                    order by brand
                """
                df_color = pd.read_sql_query(sql = query, con = conn)
                df_color['brand_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['brand']))]  
                # sqlにcolor_brandテーブルを追加もしくは置換
                df_color.to_sql('color_ideal_portfolio', conn, if_exists='replace',index=None)

        
        
        # 配当金情報がデータベース上に無い、もしくは、更新が選択された時、アップロードされたファイルからデータを取得
        if not table_isexist(conn, cur, 'stock_data') or stock_check:
            placeholder.info('株価データ読込開始')
            df_stock = pd.read_csv(stock_upload_file, encoding = "shift-jis")
            df_stock = df_stock.astype({'銘柄コード':str})
            # df_stock = df_stock.astype({'銘柄コード':str, '始値':float, '高値':float, '安値':float, '終値':float, '配当':float, 'PER':float, 'PBR':float})
            # df_stock['現在日付'] = pd.to_datetime(df_stock['現在日付'])
            # df_stock['配当落日'] = pd.to_datetime(df_stock['配当落日'])
            
            # sqlにnikkei_dataテーブルを追加もしくは置換
            df_stock.to_sql('stock_data', conn, if_exists='replace',index=None)
            placeholder.info('株価データの読込終了')
            
            # portfolio_dataがデータベースにあれば更新
            if table_isexist(conn, cur, 'portfolio_data'):
                placeholder.info('ポートフォリオデータの更新')
                update_portfolio_stock(conn, cur, table_name='portfolio_data')
                update_ideal_portfolio(conn, cur, table_name='ideal_portfolio')
                placeholder.info('ポートフォリオデータの更新終了')
        
        placeholder.info('終了')
    return



# =============================================================================
#　データベース内のtable名’table_name’にデータが存在するか確認
# =============================================================================
def table_isexist(conn, cur, table_name):
    cur.execute(f"""
        SELECT COUNT(*) FROM sqlite_master 
        WHERE TYPE='table' AND name='{table_name}'
        """)
    if cur.fetchone()[0] == 0:
        return False
    return True  

# =============================================================================
# 各ボタンが選択された時の処理
# =============================================================================
def input_sbi():
    st.session_state['sbi_id'] = st.text_input('SBI証券 ユーザーネーム')
    st.session_state['sbi_pass'] = st.text_input('SBI証券 パスワード',type='password')
    
# def input_rakuten():
#     st.session_state['rakuten_id'] = st.text_input('楽天証券 ユーザーネーム')
#     st.session_state['rakuten_pass'] = st.text_input('楽天証券 パスワード',type='password')
       
def input_neo():
    st.session_state['neo_id'] = st.text_input('ネオモバイル証券 ユーザーネーム')
    st.session_state['neo_pass'] = st.text_input('ネオモバイル証券 パスワード',type='password')

def input_sbi_csv():
    st.session_state['sbi_csv'] = st.file_uploader("SBI証券口座のデータファイル（CSV）", type='csv')

# =============================================================================
# カラーリストの作成
# =============================================================================
def create_color_list(color_list={},color_index=0,max_num=255,min_num=5,d_num=125):
    # 各色の初期値を設定（0:R,1:G,2:B）
    color_num = {0:max_num,1:min_num,2:min_num}
    # 初期値をcolor_listに格納し、インデックスに１をプラス
    color_list[color_index] = "#"+create_color_num(color_num[0])+create_color_num(color_num[1])+create_color_num(color_num[2])
    color_index += 1
    # old_colorとcurrent_colerの設定
    old_color = 0
    current_coler = 1
    while True:
        # current_colerのカラー値にd_numを加算
        color_num[current_coler]+=d_num
        # max_numを超える場合はmax_numに変更
        if color_num[current_coler]>max_num: color_num[current_coler]=max_num
        # color_listに格納し、インデックスに１をプラス
        color_list[color_index] = "#"+create_color_num(color_num[0])+create_color_num(color_num[1])+create_color_num(color_num[2])
        color_index += 1
        # 各色1巡して、min_num+50がmax_numを超えたらbreak
        if color_num[current_coler]==max_num and old_color==2 and min_num+50>=max_num: break
        # min_num+50がmax_numを超えない場合は、現在のパラメータ等を引き継いで関数create_color_listを実行し、break
        # d_numには、max_num-min_numの半分の値を使用
        elif color_num[current_coler]==max_num and old_color==2:
            min_num += 50
            d_num = math.ceil((max_num-min_num)/2)
            color_list = create_color_list(color_list=color_list,color_index=color_index,max_num=max_num,min_num=min_num,d_num=d_num)
            break
        # 一巡していない場合
        elif color_num[current_coler]==max_num:
            # color_num[old_color]を初期値に変更し、old_colorにcurrent_colerを代入。その後current_colerを変更
            color_num[old_color] = min_num
            old_color = current_coler
            current_coler = (current_coler+1)%3
            # olor_listに格納し、インデックスに１をプラス
            color_list[color_index] = "#"+create_color_num(color_num[0])+create_color_num(color_num[1])+create_color_num(color_num[2])
            color_index += 1
    return color_list

# 16進数に変換したときに、1桁しかない場合は先頭に"0"を追加
def create_color_num(color_num):
    color_num = hex(color_num)[2:]
    if len(color_num)==1: color_num = "0"+color_num
    return color_num


# =============================================================================
# SBI証券のポートフォリオデータを取得
# =============================================================================
def get_sbi_data(df_j,driver,placeholder_sbi):
    ## SBI証券のポートフォリオデータの読み込み
    with placeholder_sbi: 
        try:
            st.info('SBI証券サイトに接続中')
            ## SBI証券のトップ画面を開く    
            driver.get('https://www.sbisec.co.jp/ETGate')
            
            for i in range(5):
                # プレースホルダーに残り秒数を書き込む
                st.info(f'SBI証券サイトに接続中　　{4 - i}')
                # スリープ処理を入れる
                time.sleep(1)
            
        except:
            st.error('接続失敗')
            ## ウィンドウを閉じる
            driver.quit()
            st.stop()
            
        try:
            st.info('SBI証券ログイン中')
            ## ユーザーIDとパスワード
            input_user_id = driver.find_element_by_name('user_id')
            input_user_id.send_keys(st.session_state['sbi_id'])
            input_user_password = driver.find_element_by_name('user_password')
            input_user_password.send_keys(st.session_state['sbi_pass'])
            
            ## ログインボタンをクリック
            driver.find_element_by_name('ACT_login').click()
            
            # time.sleep(4)
            ## 遷移するまで待つ
            for i in range(5):
                # プレースホルダーに残り秒数を書き込む
                st.info(f'SBI証券ログイン中　　{4 - i}')
                # スリープ処理を入れる
                time.sleep(1)
           
            st.info('SBI証券データ取得中')
            ## ポートフォリオの画面に遷移
            driver.find_element_by_xpath('//*[@id="link02M"]/ul/li[1]/a/img').click()
            
            ## 文字コードをUTF-8に変換
            html = driver.page_source.encode('utf-8')
            ## BeautifulSoupでパース
            soup = BeautifulSoup(html,"html.parser")
            ## 株式等のデータ取得準備
            table_data = soup.find_all("table",bgcolor="#9fbf99",cellpadding="4",cellspacing="1",width="100%")
            ##資産種類と預り区分の取得準備
            account_type = soup.find_all("table",border="0",cellspacing="0",cellpadding="0",width="741")
            
        except:
            st.error('ログイン失敗：ID、パスワードを確認してください')
            st.error('失敗が続く場合は、csvファイルから取り込んでください')
            ## ウィンドウを閉じる
            driver.quit()
            st.stop()
        
        st.info('SBI証券取得データ処理中')
        df_portfolio = {}
        ## 資産種類の数だけ繰り返し
        for i in range(len(table_data)):
            #資産種類と預り区分を取得
            df_account = pd.read_html(str(account_type),header = 0)[i*2+1]
            type_and_account = list(df_account.keys())[0]
            type_and_account = re.split('[(/)]',type_and_account)
            # 預り区分の「預り」を取り除いて区分の情報を取得
            if '預り' in type_and_account[2]: account = type_and_account[2][:-2]
            else: account = type_and_account[2]
            ## 株式等のデータ取得部分を消去
            df_data = pd.read_html(str(table_data),header = 0)[i]
            if type_and_account[0]=='株式': df_portfolio[i] = domestic_stocks(df_data,category='国内株',account=account,securities_firm='SBI')
            elif type_and_account[0]=='投資信託':df_portfolio[i] = investment_trust(df_data,category=type_and_account[0],account=account,securities_firm='SBI')
        
        df_sbi = df_portfolio[0]
        ## df_portfolioが複数キーを持つ場合は結合
        if len(table_data)>1:
            for i in range(1,len(table_data)): df_sbi = pd.concat([df_sbi,df_portfolio[i]],ignore_index=True)
            df_sbi = df_sbi.reset_index(drop=True)
            
        ## 国内株式データの取り出し（コードが記入されている物の抽出）
        extract_data = ['コード','保有数量(株)','平均取得単価(円)','現在値(円)','取得総額(円)','評価額(円)','損益(円)','損益(%)','預り区分','カテゴリー']
        df_ds = df_sbi.loc[df_sbi['コード']!='',extract_data]
        ## 投資信託データの取り出し（銘柄名が記入されている物の抽出）
        extract_data = ['銘柄名','保有数量(株)','平均取得単価(円)','現在値(円)','取得総額(円)','評価額(円)','損益(円)','損益(%)','預り区分','カテゴリー']
        df_ft = df_sbi.loc[df_sbi['銘柄名']!='',extract_data]
        ## 'コード'を文字列に変換
        df_ds = df_ds.astype({'コード':str})
        
        ## 共通の'コード'を持つデータを結合
        df = pd.merge(df_j,df_ds,how="inner",on="コード")
        ## dfにdf_ftを追加
        df = pd.concat([df,df_ft],ignore_index=True) 
            
        df['証券会社'] = 'SBI証券'
        return df


# =============================================================================
# sbi証券：国内株式のポートフォリオデータを取得
# =============================================================================
def domestic_stocks(df_data,category,account,securities_firm):
    ## 銘柄（コード）を取得し、code_and_nameに代入
    code_and_name = df_data.loc[:,['銘柄（コード）']]
    ## 空白でデータ分割後に新たなDataFrameとして設定
    code_and_name = code_and_name['銘柄（コード）'].str.split('\s+',expand=True)
    ## 列名の再設定
    code_and_name.columns = ['コード','銘柄名']
    
    ## 列名を「取得総額(円)」とし、取得単価と数量から取得総額(円)を計算
    df_data['取得総額(円)'] = df_data['取得単価']*df_data['数量']
    
    ## 必要な列のみ抽出
    extract_data=['数量','取得単価','現在値','取得総額(円)','評価額','損益','損益（％）']
    df_data = df_data.loc[:, extract_data]
    
    ## カラム名を変更
    df_data = df_data.rename(columns={'数量':'保有数量(株)','取得単価':'平均取得単価(円)','現在値':'現在値(円)','評価額':'評価額(円)','損益':'損益(円)','損益（％）':'損益(%)'})
    
    ## df_dataにcode_and_name(コード・銘柄名)の追加
    df_data.insert(0,'コード',code_and_name['コード'])
    df_data.insert(1,'銘柄名','')
    
    ## カテゴリー追加
    df_data['カテゴリー'] = category
    ## 口座追加
    df_data['預り区分'] = account
    ## 証券会社追加
    df_data['証券会社'] = securities_firm
    
    return df_data

# =============================================================================
# sbi証券：投資信託のポートフォリオデータを取得
# =============================================================================
def investment_trust(df_data,category,account,securities_firm):
    ## 列名を「取得総額(円)」とし、取得単価と数量から取得総額(円)を計算
    df_data['取得総額(円)'] = df_data['取得単価']*df_data['数量']/10000
    
    ## 必要な列のみ抽出
    extract_data=['ファンド名','数量','取得単価','現在値','取得総額(円)','評価額','損益','損益（％）']
    df_data = df_data.loc[:, extract_data]
    
    ## カラム名を変更
    df_data = df_data.rename(columns={'ファンド名':'銘柄名','数量':'保有数量(株)','取得単価':'平均取得単価(円)','現在値':'現在値(円)','評価額':'評価額(円)','損益':'損益(円)','損益（％）':'損益(%)'})
    
    ## df_dataにcode_and_name(コード)の追加
    df_data.insert(0,'コード','')
    
    ## カテゴリー追加
    df_data['カテゴリー'] = category
    ## 口座追加
    df_data['預り区分'] = account
    ## 証券会社追加
    df_data['証券会社'] = securities_firm
    
    return df_data


# =============================================================================
# SBI証券のデータをCSVファイルから取得
# =============================================================================
def get_data_sbi_csv(df_j):
    if st.session_state['sbi_csv'] is not None:
        # To convert to a string based IO
        stringio = io.StringIO(st.session_state['sbi_csv'].getvalue().decode("shift-jis"))
        # To read file as string
        string_data = stringio.read()
        string_data = string_data.replace('"','')
        string_data = string_data.split(',')
        
        mode = None
        extract_data = ['コード','銘柄名','保有数量(株)','平均取得単価(円)','現在値(円)','取得総額(円)','評価額(円)','損益(円)','損益(%)','カテゴリー','預り区分','証券会社']
        portfolio_dict = {}
        for i in extract_data: portfolio_dict[i] = []
        for i in string_data:
            if mode is None:
                if '株式' in i:
                    mode = '株式'
                    classification = re.split('[/預]',i)[1]
                    counter=-10
                    continue
                elif '投資信託' in i:
                    mode = '投資信託'
                    classification = re.split('[/預]',i)[1]
                    counter=-10
                    continue
                
            elif mode == '株式' and counter>=0:
                if '合計' in i:
                    mode = None
                    continue
                
                if counter == 0:
                    portfolio_dict['コード'].append(int(i.split(' ')[0]))
                    portfolio_dict['銘柄名'].append('')
                elif counter == 2:
                    s_num = float(i)
                    portfolio_dict['保有数量(株)'].append(s_num)
                elif counter == 3:
                    g_val = float(i)
                    portfolio_dict['平均取得単価(円)'].append(g_val)
                    portfolio_dict['取得総額(円)'].append(s_num*g_val)
                elif counter == 4: portfolio_dict['現在値(円)'].append(float(i))
                elif counter == 7: portfolio_dict['損益(円)'].append(float(i))
                elif counter == 8: portfolio_dict['損益(%)'].append(float(i))
                elif counter == 9:
                    portfolio_dict['評価額(円)'].append(float(i))
                    portfolio_dict['カテゴリー'].append('国内株')
                    portfolio_dict['預り区分'].append(classification)
                    portfolio_dict['証券会社'].append('SBI証券')
                
                counter = (counter+1)%10
            
            
            elif mode == '投資信託' and counter>=0:
                if '合計' in i:
                    mode = None
                    continue
                
                if counter == 0:
                    portfolio_dict['銘柄名'].append(i.replace(' ',''))
                    portfolio_dict['コード'].append('')
                elif counter == 2:
                    s_num = float(i)
                    portfolio_dict['保有数量(株)'].append(s_num)
                elif counter == 3:
                    g_val = float(i)
                    portfolio_dict['平均取得単価(円)'].append(g_val)
                    portfolio_dict['取得総額(円)'].append(s_num*g_val/10000)
                elif counter == 4: portfolio_dict['現在値(円)'].append(float(i))
                elif counter == 7: portfolio_dict['損益(円)'].append(float(i))
                elif counter == 8: portfolio_dict['損益(%)'].append(float(i))
                elif counter == 9:
                    portfolio_dict['評価額(円)'].append(float(i))
                    portfolio_dict['カテゴリー'].append('投資信託')
                    portfolio_dict['預り区分'].append(classification)
                    portfolio_dict['証券会社'].append('SBI証券')
                
                counter = (counter+1)%10
            
            else: counter += 1
        
        df_sbi = pd.DataFrame(portfolio_dict)
        ## 国内株式データ
        extract_data = ['コード','保有数量(株)','平均取得単価(円)','現在値(円)','取得総額(円)','評価額(円)','損益(円)','損益(%)','カテゴリー','預り区分','証券会社']
        df_ds = df_sbi.loc[df_sbi['コード']!='',extract_data]
        ## 投資信託データ
        extract_data = ['銘柄名','保有数量(株)','平均取得単価(円)','現在値(円)','取得総額(円)','評価額(円)','損益(円)','損益(%)','カテゴリー','預り区分','証券会社']
        df_ft = df_sbi.loc[df_sbi['銘柄名']!='',extract_data]
        ## 'コード'を文字列に変換
        df_ds = df_ds.astype({'コード':str})
        ## 共通の'コード'を持つデータを結合
        df = pd.merge(df_j,df_ds,how="inner",on="コード")
        ## dfにdf_ftを追加
        df = pd.concat([df,df_ft],ignore_index=True)
        
        return df
    
    else:
        st.error('SBI証券ポートフォリオのファイルをアップロードしてください')
        st.stop()
                
                
# =============================================================================
# ネオモバイル証券のポートフォリオデータを取得
# =============================================================================
def get_neo_data(df_j,driver,placeholder):
    with placeholder:        
        try:
            st.info('ネオモバイル証券サイトに接続中')
            
            # ネオモバログインHP
            driver.get('https://trade.sbineomobile.co.jp/login')
            # 4秒待つ
            for i in range(5):
                # プレースホルダーに残り秒数を書き込む
                st.info(f'ネオモバイル証券サイトに接続中　　{4 - i}')
                # スリープ処理を入れる
                time.sleep(1)
            
            st.info('ネオモバイル証券ログイン中')
            # ユーザーIDとパスワード
            input_user_id = driver.find_element_by_name('username')
            input_user_password = driver.find_element_by_name('password')
            btn_login = driver.find_element_by_id('neo-login-btn')
            ## ユーザーIDとパスワードを入力して、ログインボタンをクリック
            input_user_id.send_keys(st.session_state['neo_id'])
            input_user_password.send_keys(st.session_state['neo_pass'])
            btn_login.click()
            
            ## 遷移するまで待つ
            for i in range(5):
                # プレースホルダーに残り秒数を書き込む
                st.info(f'ネオモバイル証券ログイン中　　{4 - i}')
                # スリープ処理を入れる
                time.sleep(1)
            
            st.info('ネオモバイル証券データ取得中')
            
            #ポートフォリオページにアクセス
            driver.get('https://trade.sbineomobile.co.jp/account/portfolio')
            time.sleep(2)
            
        except:
            st.error('ログイン失敗:ID、パスワードを確認してください')
            ## ウィンドウを閉じる
            driver.quit()
            # sys.exit()
            st.stop()
            
        try:
            #最下部までスクロール
            html_1 = driver.page_source
            while 1:
                #div class sp-main内をスクロール　参考https://ja.coder.work/so/java/853876
                #スクロール幅10000pixelで設定しています。3ページ読み込み程度では問題なく動きますが、上手くいかないようなら数字を大きくしてみてください。     
                driver.execute_script("arguments[0].scrollTop =arguments[1];",driver.find_element_by_class_name("sp-main"), 10000);
                
                time.sleep(2)
                html_2 = driver.page_source
                if html_1 != html_2: html_1=html_2
                else:break            
            
            # 文字コードをUTF-8に変換
            html = driver.page_source.encode('utf-8')
            # BeautifulSoupでパース
            soup = BeautifulSoup(html,'html.parser')
            
            #table(現在値〜預り区分をpandasで取得)
            table = soup.findAll('table')
            df_table = pd.read_html(str(table))
            #table数（保持銘柄数）
            table_no = len(df_table)
            
            #空のデータフレームを作成
            list_df = pd.DataFrame()
            #銘柄ごとにテーブルを取得し追加していく
            for i in range(0,table_no):
                s = df_table[i].iloc[:,1]
                list_df = list_df.append(s)
            
            #列名の付け直し
            list_df.columns=['現在値前日比','保有数量','（うち売却注文中）','評価損益率','平均取得単価','預り区分'] 
            #indexを0から振る
            list_df = list_df.reset_index(drop=True)
            
            #コード
            code_list = list()
            for codes in soup.find_all(class_="ticker"):
                code = codes.get_text()
                code = code.strip()
                code_list.append(code)
                
            df_code = pd.Series(code_list)
            
            # #銘柄名
            # name_list = list()
            # for names in soup.find_all(class_="name"):
            #     name = names.get_text()
            #     name = name.strip()
            #     name_list.append(name)
                
            # df_name = pd.Series(name_list)
            
            #評価額
            value_list = list()
            for values in soup.find_all(class_="value"):
                value = values.get_text()
                value_list.append(value)
                
            df_value = pd.Series(value_list)
            
            #評価損益
            rate_list = list()
            for rates in soup.find_all(class_="rate"):
                rate = rates.get_text().strip()
                rate_list.append(rate)
            
            df_rate = pd.Series(rate_list)
            
            #コード〜評価損益を結合
            df = pd.concat([df_code,df_value,df_rate],axis=1)
            #列名を更新
            df.columns = ['コード','評価額','損益']
            
            df_result = pd.concat([df,list_df],axis=1)
            df_result.index = df_result.index+1
            
            #数値を扱いやすいように修正
            #評価額
            value = df_result["評価額"].str.split("\n",expand=True)[1]
            value = value.str.replace(',','')
            
            #損益
            rate = df_result["損益"].str.split("\n",expand=True)[2]
            rate = rate.str.replace(',','')
            rate = rate.str.replace(' ','')
            
            #現在値
            price = df_result["現在値前日比"].str.split("円",expand=True)[0]
            price = price.str.replace(',','')
            
            # #前日比円
            # pricerate=df_result["現在値前日比"].str.split("/  ",expand=True)[1]
            # pricerate=pricerate.str.split(" ",expand=True)[0]
            # pricerate=pricerate.str.replace(',','')
        
            # #前日比パーセント
            # pricepercent=df_result["現在値前日比"].str.split("/  ",expand=True)[1]
            # pricepercent=pricepercent.str.split(" ",expand=True)[1]
            # pricepercent=pricepercent.str.split("%",expand=True)[0]
            
            #保有数量
            stock = df_result["保有数量"].str.split("株",expand=True)[0]
            stock = stock.str.replace(',','')
            
            # #売却注文中
            # stocksell=df_result["（うち売却注文中）"].str.split("株",expand=True)[0]
            # stocksell=stocksell.str.replace(',','')
    
            #損益
            percentage = df_result["評価損益率"].str.split("%",expand=True)[0]
            
            #平均取得単価
            aveprice = df_result["平均取得単価"].str.split(" ",expand=True)[0]
            aveprice = aveprice.str.replace(',','')
            
            #データフレームを結合
            df_result2 = pd.concat([df_result["コード"],value,rate,price,stock,percentage,aveprice,df_result["預り区分"]],axis=1)
            df_result2.columns = ['コード','評価額(円)','損益(円)','現在値(円)','保有数量(株)','損益(%)','平均取得単価(円)','預り区分']
            
            ## カテゴリー追加
            df_result2['カテゴリー'] = '国内株'
            ## 証券会社追加
            df_result2['証券会社'] = 'ネオモバイル'
            
            df_result3 = pd.merge(df_j,df_result2,how="inner",on="コード")
            
            st.write('ネオモバイル証券データ読み込み終了')
            
            return df_result3
            
        except:
            st.error('読み込み失敗')
            ## ウィンドウを閉じる
            driver.quit()
            # sys.exit()
            st.stop()


# =============================================================================
# ポートフォリオの株価データの更新    
# =============================================================================
def update_portfolio_stock(conn, cur, table_name):
    # DBからコード、保有数、取得総額、終値を取得し、各種データの計算
    query = f"""
        select {table_name}.code, {table_name}.posession_num, {table_name}.total_acquisition_yen,(case stock_data.終値 when ' ' then '0' else stock_data.終値 end) as 終値
        from {table_name}, stock_data
        where {table_name}.code == stock_data.銘柄コード
    """
    df_stock = pd.read_sql_query(sql = query, con = conn)
    df_stock = df_stock.astype({'posession_num':float,'total_acquisition_yen':float,'終値':float})
    df_stock['valuation_yen'] = df_stock['posession_num']*df_stock['終値']
    df_stock['PL_yen'] = df_stock['valuation_yen']-df_stock['total_acquisition_yen']
    df_stock['PL_per'] = 100*df_stock['PL_yen']/df_stock['total_acquisition_yen']
    df_stock = df_stock.astype({'valuation_yen':str, 'PL_yen':str, 'PL_per':str})
    
    # DBのアップデート
    for i in range(len(df_stock['code'])):
        query = f"""
            update {table_name}
            set valuation_yen = {df_stock['valuation_yen'][i]}, PL_yen = {df_stock['PL_yen'][i]}, PL_per = {df_stock['PL_per'][i]}
            where code = {df_stock['code'][i]}
        """
        # データ更新
        cur.execute(query)
        # コミット
        conn.commit()
    return

# =============================================================================
# 理想のポートフォリオの株価データの更新    
# =============================================================================
def update_ideal_portfolio(conn, cur, table_name):
    # DBからコード、保有数、取得総額、終値を取得し、各種データの計算
    query = f"""
        select {table_name}.code, {table_name}.posession_num, (case stock_data.終値 when ' ' then '0' else stock_data.終値 end) as 終値, (case stock_data.配当 when '-' then '0' else stock_data.配当 end) as 配当
        from {table_name}, stock_data
        where {table_name}.code == stock_data.銘柄コード
    """
    df_stock = pd.read_sql_query(sql = query, con = conn)
    df_stock = df_stock.astype({'posession_num':float, '終値':float, '配当':float})
    df_stock['valuation_yen'] = df_stock['posession_num']*df_stock['終値']
    df_stock['税引前配当額'] = df_stock['posession_num']*df_stock['配当']
    df_stock = df_stock.astype({'valuation_yen':str, '税引前配当額':str})
    
    # DBのアップデート
    for i in range(len(df_stock['code'])):
        query = f"""
            update {table_name}
            set valuation_yen = {df_stock['valuation_yen'][i]}
            where code = {df_stock['code'][i]}
        """
        # データ更新
        cur.execute(query)
        # コミット
        conn.commit()
    return

# =============================================================================
# モード：ポートフォリオ表示
# =============================================================================
def my_portfolio(conn, cur):
    # # 日経銘柄情報を読み込む必要があるのに、ファイルがアップロードされていない時にエラー文を表示
    # if not table_isexist(conn, cur, 'nikkei_data'):
    #     st.error('「データ取得・更新」から日経銘柄データを登録してください')
    #     st.stop()
    
    # st.header('ポートフォリオ表示')
    # DBにportfolio_dataが無い場合はエラー文を表示
    if not table_isexist(conn, cur, 'portfolio_data'):
        st.error('モード「データ取得・更新」からポートフォリオデータを取得してください')
        st.stop()
    
    st.header("保有資産表示")
    # portfolio_data表の列名設定項目をデータベースから読込
    df_column_dict = {}
    query = """
        select * from portfolio_data_column
    """
    for i in cur.execute(query): df_column_dict[i[1]] = i[0]

    # 表示項目等の設定
    select_data_dict = {'評価額(円)':'valuation_yen','取得総額(円)':'total_acquisition_yen'}
    select_target_dict = {'カテゴリー別':'category','証券会社別':'securities_account','預り区分別':'classification','17業種区分（国内株）':'industry17','33業種区分（国内株）':'industry33'}
    # 表示項目をセレクトボックスで選択1
    select_data = st.sidebar.selectbox("表示内容の選択",tuple(select_data_dict.keys()))
    # 表示項目をセレクトボックスで選択2
    select_target = st.sidebar.selectbox("分別項目選択",tuple(select_target_dict.keys()))
    # select_target_dict,select_target_dictを使用してdfに対応するワードに変更
    select_data = select_data_dict[select_data]
    select_target = select_target_dict[select_target]
    
    # データベースからbrandとselect_targetのカラー一覧を取得
    df_color = {}
    query = """
        select * from color_brand
    """
    for i in cur.execute(query): df_color[i[0]] = i[1]
    
    query = f"""
        select * from color_{select_target}
    """
    for i in cur.execute(query): df_color[i[0]] = i[1]
    
    # データベースから表示するデータの抜出
    # select_list = [select_target,'brand','posession_num','total_acquisition_yen','valuation_yen','PL_yen']
    # select_query = create_sql_doc(select_list)
    
    query = f"""
        select portfolio_data.{select_target}, portfolio_data.brand, portfolio_data.posession_num,
        portfolio_data.total_acquisition_yen, portfolio_data.valuation_yen, portfolio_data.PL_yen,
        (case when stock_data.配当=='-' then '0' when portfolio_data.category=='投資信託' then '0' else stock_data.配当 end) as 配当
        from portfolio_data
        left join stock_data
        on portfolio_data.code == stock_data.銘柄コード 
    """
    df_data = pd.read_sql_query(sql = query, con = conn)
    df_data = df_data.astype({'配当':float})
    df_data = df_data.sort_values([select_target, select_data],ascending=[True, False])
    df_data = df_data.reset_index()
    df_data = df_data.rename(columns = df_column_dict)
    
    # select_targetの一覧をサイドバーに表示する準備
    # データベースからselect_targetの一覧を取得
    query = f"""
        select {select_target} from portfolio_data
        where {select_target} is not NULL
        group by {select_target}
        order by {select_target}
    """
    items_list = pd.read_sql_query(sql = query, con = conn)
    items_list = list(items_list.loc[:,select_target])
    # マルチセレクトボックスをサイドバーに表示（デフォルトですべて選択）
    selected_items  = st.sidebar.multiselect("詳細選択", items_list, default=items_list)
    if not selected_items:
        st.info('詳細選択の項目を設定してください')
        st.stop()
    
    # df_graphの作成
    # select_targetの項目が、selected_itemsに含まれる物を抜き出し、select_targetでグループ化し、同じselect_targetの各数値を合計
    select_list = ['brand','sum(posession_num) as posession_num','sum(total_acquisition_yen) as total_acquisition_yen','sum(valuation_yen) as valuation_yen','sum(PL_yen) as PL_yen',select_target]
    select_query = create_sql_doc(select_list)
    where_query = create_sql_doc(selected_items,add="'")
    
    if select_target in ['category','securities_account','classification']:
        query = f"""
            select {select_query} from portfolio_data
            where {select_target} in ({where_query})
            group by {select_target},brand
            order by {select_target} asc, {select_data} desc
        """
        
    elif select_target in ['industry17','industry33']:
        query = f"""
            select {select_query} from portfolio_data
            where {select_target} in ({where_query}) and category=='国内株'
            group by {select_target},brand
            order by {select_target} asc, {select_data} desc
        """
    df_graph = pd.read_sql_query(sql = query, con = conn)
    df_graph = df_graph.sort_values([select_target, select_data],ascending=[True, False])
    df_graph = df_graph.reset_index()
    df_graph = df_graph.rename(columns = df_column_dict)
    
    # df_target_groupの作成
    # select_targetの項目が、selected_itemsに含まれる物を抜き出し、select_targetでグループ化し、同じselect_targetの各数値を合計
    select_list = [select_target,'sum(posession_num) as posession_num','sum(total_acquisition_yen) as total_acquisition_yen','sum(valuation_yen) as valuation_yen','sum(PL_yen) as PL_yen']
    select_query = create_sql_doc(select_list)
    where_query = create_sql_doc(selected_items,add="'")
        
    if select_target in ['category','securities_account','classification']:
        query = f"""
            select {select_query} from portfolio_data
            where {select_target} in ({where_query})
            group by {select_target}
            order by {select_target}
        """
        
    elif select_target in ['industry17','industry33']:
        query = f"""
            select {select_query} from portfolio_data
            where {select_target} in ({where_query}) and category=='国内株'
            group by {select_target}
            order by {select_target}
        """
    
    df_target_group = pd.read_sql_query(sql = query, con = conn)
    df_target_group = df_target_group.sort_values([select_target],ascending=[True])
    df_target_group = df_target_group.reset_index()
    df_target_group = df_target_group.rename(columns = df_column_dict)

    # # df_targetの作成
    # # select_targetの項目が、selected_itemsに含まれる物を抜き出し、'銘柄名'でグループ化し、合計値を計算
    # select_list = ['brand','sum(total_acquisition_yen) as total_acquisition_yen','sum(valuation_yen) as valuation_yen','sum(PL_yen) as PL_yen']
    # select_query = create_sql_doc(select_list)
    # where_query = create_sql_doc(selected_items,add="'")
    
    # query = f"""
    #     select {select_query} from portfolio_data
    #     where {select_target} in ({where_query}) and category=='国内株'
    #     group by brand
    #     order by sum(total_acquisition_yen) desc
    # """
    
    # df_target = pd.read_sql_query(sql = query, con = conn)
    # df_target = df_target.rename(columns = df_column_dict)


    # df_column_dictを使用してselect_data,select_targetをdf_dataに対応するワードに変更
    select_data = df_column_dict[select_data]
    select_target = df_column_dict[select_target]

    # '資産割合(%)'を計算
    df_graph['資産割合(%)'] = 100*df_graph[select_data]/df_graph[select_data].sum()
    df_target_group['資産割合(%)'] = 100*df_target_group[select_data]/df_target_group[select_data].sum()
    
    
    # グラフ・表の表示
    st.subheader(f'保有割合({select_target}・{select_data})')
    plot_2piegraph_table(title="ポートフォリオ全体", df_data=df_graph, data=select_data, df_target=df_target_group, target=select_target, df_color=df_color)
    st.subheader(f'{select_target}別の合計額・割合')
    plot_bargraph_table(title="target", df_target=df_target_group, target=select_target, margin=0.4)
    # st.subheader('銘柄名別の合計額・割合')
    # plot_bargraph_table(title="target", df_target=df_target, target='銘柄名', margin=0.4)
    
    # '銘柄名'でグループ化して値を合計する
    df_data = df_data.groupby([select_target,'銘柄名'],as_index=False).agg({'保有数量(株)':'sum', '取得総額(円)':'sum','評価額(円)':'sum', '損益(円)':'sum','配当':'mean'})
    df_data['損益(%)'] = (df_data['評価額(円)']-df_data['取得総額(円)'])/df_data['評価額(円)']*100
    df_data['税引前配当額(円)'] = df_data['保有数量(株)']*df_data['配当']
    # 詳細データの出力
    st.subheader('詳細データ')
    df_data = df_data.append(df_data.sum(numeric_only=True), ignore_index=True)
    df_data.loc[len(df_data)-1,'銘柄名'] = ["合計"]
    df_data.loc[len(df_data)-1,'保有数量(株)'] = [None]
    df_data.loc[len(df_data)-1,'損益(%)'] = (df_data.loc[len(df_data)-1,'評価額(円)']-df_data.loc[len(df_data)-1,'取得総額(円)'])/df_data.loc[len(df_data)-1,'取得総額(円)']
    df_data.insert(3,'取得単価(円)',df_data.loc[:,'取得総額(円)']/df_data.loc[:,'保有数量(株)'])
    formatter={('保有数量(株)'):"{:,.0f}",('取得総額(円)'):"{:,.1f}",('取得単価(円)'):"{:,.1f}",('評価額(円)'):"{:,.1f}",('損益(円)'):"{:,.1f}",('損益(%)'):"{:.2f}",('資産割合(%)'):"{:.2f}",('税引前配当額(円)'):"{:,.1f}"}
    df_data = df_data.loc[:,[select_target,'銘柄名','保有数量(株)','取得総額(円)','評価額(円)','損益(円)','損益(%)','税引前配当額(円)']]
    df_data = df_data.style.format(formatter=formatter,na_rep="-")
    st.dataframe(df_data,height=500)
    return


# =============================================================================
# モード：理想のポートフォリオ
# =============================================================================
def ideal_portfolio(conn,cur):
    if not table_isexist(conn, cur, 'ideal_portfolio'):
        # DBにportfolio_dataが無い場合はエラー文を表示
        if not table_isexist(conn, cur, 'ideal_portfolio'):
            st.error('モード「データ取得・更新」もしくは「国内株銘柄検索（業種別）・登録」から登録してください')
            st.stop()
    
    # portfolio_data表の列名設定項目をデータベースから読込
    df_column_dict = {}
    query = """
        select * from portfolio_data_column
    """
    for i in cur.execute(query): df_column_dict[i[1]] = i[0]
    
    # 表示項目等の設定
    select_dict = {'17業種区分':'industry17','33業種区分':'industry33','評価額(円)':'valuation_yen','株数':'posession_num'}
    # 表示項目をセレクトボックスで選択1
    select_data = st.sidebar.selectbox("表示内容の選択",tuple(['評価額(円)','株数']))
    # 表示項目をセレクトボックスで選択2
    select_target = st.sidebar.selectbox("分別項目選択",tuple(['17業種区分','33業種区分']))
    # 表示項目をセレクトボックスで選択3
    select_sort = st.sidebar.selectbox("ソート項目選択",tuple([select_target,select_data]))
    # # 円グラフのタイトル用
    # select_data0 = select_data
    # select_target0 = select_target
    # select_dictを使用してdfに対応するワードに変更
    select_data = select_dict[select_data]
    select_target = select_dict[select_target]
    select_sort = select_dict[select_sort]
    
    # データベースからideal_portfolioとselect_targetのカラー一覧を取得
    df_color = {}
    query = """
        select brand, brand_color from color_ideal_portfolio
    """
    for i in cur.execute(query): df_color[i[0]] = i[1]
    
    query = f"""
        select * from color_{select_target}
    """
    for i in cur.execute(query): df_color[i[0]] = i[1]
     
    # データベースから表示するデータの抜出
    query = """
         select * from ideal_portfolio
     """
    df_data = pd.read_sql_query(sql = query, con = conn)
    if len(df_data['code'])==0:
        st.error('モード「データ取得・更新」もしくは「国内株銘柄検索（業種別）・登録」から登録してください')
        st.stop()
    df_data = df_data.astype({f'{select_data}':float,'株価':float,'配当':float})
    if select_sort == select_target: df_data = df_data.sort_values([select_target, select_data],ascending=[True, False])
    elif select_sort == select_data: df_data = df_data.sort_values([select_data, select_target],ascending=[False, True])
    df_data = df_data.reset_index()
    df_data['資産割合(%)'] = 100*df_data[select_data]/df_data[select_data].sum()
    df_data = df_data.rename(columns = df_column_dict)
    df_data = df_data.rename(columns = {'保有数量(株)':'数量(株)','株価':'株価(円)','配当':'一株配当(円)','税引前配当額':'税引前配当額(円)'})
    # 編集画面用にdf_editを作成
    df_edit = df_data
    
    # グラフ表示用のチェックボックスを表示
    st.header("理想のポートフォリオ（国内株）表示・編集")
    st.subheader('理想のポートフォリオ')
    check_graph = st.checkbox('グラフを表示')
    if check_graph:
        # select_targetの一覧をサイドバーに表示する準備
        # データベースからselect_targetの一覧を取得
        query = f"""
            select {select_target} from color_{select_target}
            order by {select_target}
        """
        items_list = pd.read_sql_query(sql = query, con = conn)
        items_list = list(items_list.loc[:,select_target])
        # マルチセレクトボックスをサイドバーに表示（デフォルトですべて選択）
        selected_items  = st.sidebar.multiselect("詳細選択", items_list, default=items_list)
        if not selected_items: st.info('詳細選択の項目を設定してください')
        else:
            # df_graphの作成
            # select_targetの項目が、selected_itemsに含まれる物を抜き出し、select_targetでグループ化し、同じselect_targetの各数値を合計
            select_list = ['brand','sum(posession_num) as posession_num','sum(valuation_yen) as valuation_yen',select_target]
            select_query = create_sql_doc(select_list)
            where_query = create_sql_doc(selected_items,add="'")
            
            if select_sort == select_target:
                query = f"""
                    select {select_query} from ideal_portfolio
                    where {select_target} in ({where_query})
                    group by {select_target},brand
                    order by {select_target} asc, {select_data} desc
                """
            elif select_sort == select_data:
                query = f"""
                    select {select_query} from ideal_portfolio
                    where {select_target} in ({where_query})
                    group by {select_target},brand
                    order by {select_data} desc, {select_target} asc
                """
            df_graph = pd.read_sql_query(sql = query, con = conn)
            if len(df_graph[select_target])>0:
                show_graph = True
                df_graph = df_graph.reset_index()
                df_graph['資産割合(%)'] = 100*df_graph[select_data]/df_graph[select_data].sum()
                df_graph = df_graph.rename(columns = {'posession_num':'数量(株)'})
                df_graph = df_graph.rename(columns = df_column_dict)
            else: show_graph = False
            # df_target_groupの作成
            # select_targetの項目が、selected_itemsに含まれる物を抜き出し、select_targetでグループ化し、同じselect_targetの各数値を合計
            select_list = [select_target,'sum(posession_num) as posession_num', 'sum(valuation_yen) as valuation_yen']
            select_query = create_sql_doc(select_list)
            
            if select_sort == select_target:
                query = f"""
                    select {select_query} from ideal_portfolio
                    where {select_target} in ({where_query})
                    group by {select_target}
                    order by {select_target}
                """
            elif select_sort == select_data:
                query = f"""
                    select {select_query} from ideal_portfolio
                    where {select_target} in ({where_query})
                    group by {select_target}
                    order by {select_data} desc
                """
            df_target_group = pd.read_sql_query(sql = query, con = conn)
            if len(df_target_group[select_target])>0:
                df_target_group = df_target_group.reset_index()
                df_target_group['資産割合(%)'] = 100*df_target_group[select_data]/df_target_group[select_data].sum()
                df_target_group = df_target_group.rename(columns = df_column_dict)
                df_target_group = df_target_group.rename(columns = {'保有数量(株)':'数量(株)'})
            
            # データベースから表示するデータの抜出
            if select_sort == select_target:
                query = f"""
                    select color_{select_target}.{select_target}, sum(COALESCE(ideal_portfolio.{select_data},0)) as {select_data}
                    from color_{select_target}
                        left join ideal_portfolio
                        on color_{select_target}.{select_target}==ideal_portfolio.{select_target}
                    group by color_{select_target}.{select_target}
                    order by color_{select_target}.{select_target}
                """
            elif select_sort == select_data:
                query = f"""
                    select color_{select_target}.{select_target}, sum(COALESCE(ideal_portfolio.{select_data},0)) as {select_data}
                    from color_{select_target}
                        left join ideal_portfolio
                        on color_{select_target}.{select_target}==ideal_portfolio.{select_target}
                    group by color_{select_target}.{select_target}
                    order by {select_data} desc
                """
            df_graph2 = pd.read_sql_query(sql = query, con = conn)
            # '資産割合(%)'を計算
            df_graph2['資産割合(%)'] = 100*df_graph2[select_data]/df_graph2[select_data].sum()
            # df_graph2 = df_graph2.sort_values([select_data],ascending=[False])
            df_graph2 = df_graph2.reset_index(drop=True)
            # df_graph2列名の再設定
            df_graph2 = df_graph2.rename(columns = {'posession_num':'数量(株)'})
            df_graph2 = df_graph2.rename(columns = df_column_dict)
        
            # # df_column_dictを使用してselect_data,select_targetをdf_dataに対応するワードに変更
            # select_data0 = df_column_dict[select_data]
            # select_target0 = df_column_dict[select_target]
    
    # df_column_dictを使用してselect_data,select_targetをdf_dataに対応するワードに変更
    if select_data=='posession_num': select_data='数量(株)'
    else: select_data = df_column_dict[select_data]
    select_target = df_column_dict[select_target]
    
    # グラフ・表の表示
    if check_graph and selected_items:
        if show_graph:
            st.subheader(f'割合({select_target}・{select_data})')
            plot_2piegraph_table(title="理想のポートフォリオ", df_data=df_graph, data=select_data, df_target=df_target_group, target=select_target, df_color=df_color, label=False, linewidth=0.4)
        else: st.info('登録銘柄はありません')
        st.subheader(f'{select_target}別の割合')
        # グラフ・表の表示
        # st.subheader(f'理想のポートフォリオ({select_target0}・{select_data0})')
        plot_1piegraph_table(title="理想のポートフォリオ", df_data=df_graph2, data=select_data, target=select_target, df_color=df_color)
    
    # 詳細データの出力
    st.subheader('詳細')
    df_data = df_data.append(df_data.sum(numeric_only=True), ignore_index=True)
    df_data.loc[len(df_data)-1,'銘柄名'] = ["合計"]
    df_data.loc[len(df_data)-1,'数量(株)'] = [None]
    df_data.loc[len(df_data)-1,'株価(円)'] = [None]
    df_data.loc[len(df_data)-1,'評価額(円)'] = df_data['評価額(円)'].sum()
    df_data.loc[len(df_data)-1,'税引前配当額(円)'] = df_data['税引前配当額(円)'].sum()
    df_data['配当利回り(%)'] = df_data['税引前配当額(円)']/df_data['評価額(円)']*100
    df_data = df_data.loc[:,[select_target,'銘柄名','株価(円)','数量(株)','評価額(円)','一株配当(円)','税引前配当額(円)','配当利回り(%)']]
    formatter={('株価(円)'):"{:,.1f}",('数量(株)'):"{:,.0f}",('評価額(円)'):"{:,.1f}",('一株配当(円)'):"{:,.1f}",('税引前配当額(円)'):"{:.1f}",('配当利回り(%)'):"{:.2f}"}
    df_data = df_data.style.format(formatter=formatter,na_rep="-")
    st.dataframe(df_data,height=500)
    
    
    # #編集画面作成
    # DBのportfolio_dataからnan以外のcodeを抜き出し
    if table_isexist(conn, cur, 'portfolio_data'):
        query = """
            select code
            from portfolio_data
            where code is not 'nan'
            group by code
        """
        df_prop = pd.read_sql_query(sql = query, con = conn)
    
    else:
        df_prop = {'code':str(0)}
    
    st.subheader('編集（株式数の変更・削除）')
    # 銘柄選択画面を表示
    row01_spacer1,row01_1,row01_spacer2 = st.columns((0.2,2,4.4))
    _lock = RendererAgg.lock
    with row01_1,_lock:
        select_code = st.selectbox('銘柄を選択してください',list(df_edit['コード']+' '+df_edit["銘柄名"]))
        select_code = str(select_code.split(' ')[0])
    # # 画面を横に区切って編集画面を表示
    # 選択された業種のコードと銘柄名を出力(左側)
    row02_spacer1,row02_1,row02_spacer2 = st.columns((0.2,3,3.4))
    _lock = RendererAgg.lock
    with row02_1,_lock:
        # 株式数の変更
        stock_num = st.number_input(label='株式数の変更',value=list(df_edit.loc[df_edit['コード']==select_code,'数量(株)'])[0], min_value=1)
        stock_price_sum = stock_num * list(df_edit.loc[df_edit['コード']==select_code,'株価(円)'])[0]
        dividend_price = stock_num * list(df_edit.loc[df_edit['コード']==select_code,'一株配当(円)'])[0]
        if st.button('変更する'):
            query = f"""
                update ideal_portfolio
                set posession_num={int(stock_num)}, valuation_yen={stock_price_sum}, 税引前配当額={dividend_price}
                where code = {select_code}
            """
            # データ更新
            cur.execute(query)
            # コミット
            conn.commit()
            st.info('画面を更新してください')
            
        
        # 銘柄の消去
        if select_code in list(df_prop['code']):
            st.info('保有株式は削除できません')
            
        elif st.button('理想のポートフォリオから削除'):
            query = f"""
                delete from ideal_portfolio
                where code = {select_code}
            """
            # データ削除実行
            cur.execute(query)
            # コミット
            conn.commit()
            st.info('画面を更新してください')
    return


# =============================================================================
# モード：国内株銘柄検索（業種別）
# =============================================================================
def search_domestic_stock(conn, cur):
    st.header("国内株銘柄検索（業種別）・理想のポートフォリオ登録")
    st.subheader('グラフの表示')
    # 表示項目等の設定
    select_dict = {'17業種区分':'industry17','33業種区分':'industry33','評価額(円)':'valuation_yen'}
    # # 表示項目をセレクトボックスで選択1
    # select_data = st.sidebar.selectbox("表示内容の選択",['評価額(円)','取得総額(円)'])
    # 表示項目をセレクトボックスで選択2
    select_target = st.sidebar.selectbox("業種区分選択",['17業種区分','33業種区分'])
    # 表示項目をセレクトボックスで選択3
    select_sort = st.sidebar.selectbox("グラフのソート項目選択",tuple([select_target,'評価額(円)']))
    # select_target_dict,select_target_dictを使用してdfに対応するワードに変更
    select_data = 'valuation_yen'
    select_target = select_dict[select_target]
    select_sort = select_dict[select_sort]
    
    # 業種選択画面をサイドバーに表示
    query = f"""
        select {select_target}
        from nikkei_data
        where {select_target} != '-'
        group by {select_target}
        order by {select_target}
    """
    df_select = pd.read_sql_query(sql = query, con = conn)
    # select_targetの項目をサイドバーにセレクトボックス形式で表示
    items_list = list(df_select.loc[:,select_target])
    select_industry  = st.sidebar.selectbox("業種選択", items_list)
    # データベースから選択された業種のコード、銘柄名、終値、配当、PER、PBR、現在日付、配当落日を抽出
    query = f"""
        select nikkei_data.コード, nikkei_data.銘柄名, nikkei_data.IR_BANK, (case stock_data.終値 when ' ' then '0.0001' else stock_data.終値 end) as 株価（円）,
        (case stock_data.配当 when '-' then '0' else stock_data.配当 end) as 配当（円）, stock_data.PER, stock_data.PBR, stock_data.現在日付, stock_data.配当落日
        from nikkei_data, stock_data
        where {select_target} == '{select_industry}' and nikkei_data.コード == stock_data.銘柄コード
    """
    df_industry = pd.read_sql_query(sql = query, con = conn)
    df_industry = df_industry.astype({'株価（円）':float, '配当（円）':float, 'PER':float, 'PBR':float})
    df_industry['配当利回り（％）'] = df_industry['配当（円）']/df_industry['株価（円）']*100
    df_industry.loc[df_industry['配当利回り（％）'] >100, '配当利回り（％）'] = 0
    for i in range(len(df_industry['現在日付'])):
        if '-' not in df_industry['現在日付']:
            date = df_industry['現在日付'][i] 
            break
    
    # DBにportfolio_dataが存在する場合はグラフと表も表示
    if table_isexist(conn, cur, 'portfolio_data'): prop_check = st.checkbox('保有国内株式')
    else: prop_check = False
    if table_isexist(conn, cur, 'ideal_portfolio'): ideal_check = st.checkbox('理想のポートフォリオ')
    else: ideal_check = False
    
    if prop_check or ideal_check:
        # portfolio_data表の列名設定項目をデータベースから読込
        df_column_dict = {}
        query = """
            select * from portfolio_data_column
        """
        for i in cur.execute(query): df_column_dict[i[1]] = i[0]
        
        # データベースからbrandとselect_targetのカラー一覧を取得
        df_color = {}    
        query = f"""
            select * from color_{select_target}
        """
        for i in cur.execute(query): df_color[i[0]] = i[1]
        
        
    if prop_check:
        # データベースからselect_dataとselect_dataの情報を取得
        # 値が無いところはCOALESCEで0に置換
        if select_sort == select_target:
            query = f"""
                select color_{select_target}.{select_target}, sum(COALESCE(portfolio_data.{select_data},0)) as {select_data}
                from color_{select_target}
                    left join portfolio_data
                    on color_{select_target}.{select_target}==portfolio_data.{select_target}
                group by color_{select_target}.{select_target}
                order by color_{select_target}.{select_target}
            """
        elif select_sort == select_data:
            query = f"""
                select color_{select_target}.{select_target}, sum(COALESCE(portfolio_data.{select_data},0)) as {select_data}
                from color_{select_target}
                    left join portfolio_data
                    on color_{select_target}.{select_target}==portfolio_data.{select_target}
                group by color_{select_target}.{select_target}
                order by {select_data} desc
            """
        df_data = pd.read_sql_query(sql = query, con = conn)
        # '資産割合(%)'を計算
        df_data['資産割合(%)'] = 100*df_data[select_data]/df_data[select_data].sum()
        # df_data = df_data.sort_values([select_data],ascending=[False])
        df_data = df_data.reset_index(drop=True) 
        # df_data列名の再設定
        df_data = df_data.rename(columns = df_column_dict)
        
        # df_column_dictを使用してselect_data,select_targetをdf_dataに対応するワードに変更
        select_data0 = df_column_dict[select_data]
        select_target0 = df_column_dict[select_target]
        
        # グラフの表示
        st.subheader(f'保有国内株式({select_target0}・{select_data0})')
        plot_1piegraph_table(title="ポートフォリオ全体", df_data=df_data, data=select_data0, target=select_target0, df_color=df_color)
        st.write('\n')
    
    if ideal_check:
        # データベースから表示するデータの抜出
        if select_sort == select_target:
            query = f"""
                select color_{select_target}.{select_target}, sum(COALESCE(ideal_portfolio.{select_data},0)) as {select_data}
                from color_{select_target}
                    left join ideal_portfolio
                    on color_{select_target}.{select_target}==ideal_portfolio.{select_target}
                group by color_{select_target}.{select_target}
                order by color_{select_target}.{select_target}
            """
        elif select_sort == select_data:
            query = f"""
                select color_{select_target}.{select_target}, sum(COALESCE(ideal_portfolio.{select_data},0)) as {select_data}
                from color_{select_target}
                    left join ideal_portfolio
                    on color_{select_target}.{select_target}==ideal_portfolio.{select_target}
                group by color_{select_target}.{select_target}
                order by {select_data} desc
            """
        df_data = pd.read_sql_query(sql = query, con = conn)
        # '資産割合(%)'を計算
        df_data['資産割合(%)'] = 100*df_data[select_data]/df_data[select_data].sum()
        # df_data = df_data.sort_values([select_data],ascending=[False])
        df_data = df_data.reset_index(drop=True)
        # df_data列名の再設定
        df_data = df_data.rename(columns = df_column_dict)
        
        # df_column_dictを使用してselect_data,select_targetをdf_dataに対応するワードに変更
        select_data0 = df_column_dict[select_data]
        select_target0 = df_column_dict[select_target]
        
        # グラフ・表の表示
        st.subheader(f'理想のポートフォリオ({select_target0}・{select_data0})')
        plot_1piegraph_table(title="理想のポートフォリオ", df_data=df_data, data=select_data0, target=select_target0, df_color=df_color)

        
    
    # # 画面を横に区切ってグラフと表を表示
    # 選択された業種のコードと銘柄名を出力(左側)
    row0_spacer1,row0_1,row0_spacer2,row0_2,row0_spacer3 = st.columns((0.2,4,0.2,2,0.2))
    _lock = RendererAgg.lock
    with row0_1,_lock:
        st.subheader(f"{select_industry}　{len(df_industry['コード'])}銘柄\n株価取得日：{date}")
        st.dataframe(df_industry.loc[:,['コード','銘柄名','株価（円）','配当（円）','配当利回り（％）','PER','PBR','配当落日']].style.format(formatter={('株価（円）'):"{:,.1f}",('PER'):"{:,.2f}",('PBR'):"{:,.2f}",('配当（円）'):"{:,.2f}",('配当利回り（％）'):"{:,.2f}"}),height=500)
    
    # 銘柄名とIRBANK用URLの表示
    with row0_2:
        st.subheader('IR BANKで検索・登録')
        select_code = st.selectbox('銘柄を選択してください',list(df_industry['コード']+' '+df_industry["銘柄名"]))
        query = f"""
            select 銘柄名,IR_BANK from nikkei_data
            where コード == '{select_code[:4]}' 
        """
        df_select = pd.read_sql_query(sql = query, con = conn)
        link = f'[{df_select["銘柄名"][0]}]({df_select["IR_BANK"][0]})'
        st.write('「IR BANK」サイトリンク')
        st.markdown(link, unsafe_allow_html=True)
        
        if table_isexist(conn, cur, 'ideal_portfolio'):
            query = """
                select code
                from ideal_portfolio
            """
            ideal_code_list = list(pd.read_sql_query(sql = query, con = conn)['code'])
            
            if select_code[:4] in ideal_code_list:
                st.info(f'{select_code}は登録済みです')
                st.stop()
            
        if st.button(f'「{select_code.split(" ")[1]}」を理想のポートフォリオに登録'):
            if not table_isexist(conn, cur, 'ideal_portfolio'):
                query = f"""
                    select コード, 銘柄名, 1 as posession_num, (case 終値 when ' ' then '0.0001' else 終値 end) as valuation_yen, industry17, industry33,
                    (case 終値 when ' ' then '0.0001' else 終値 end) as 株価, (case 配当 when '-' then '0' else 配当 end) as 配当, (case 配当 when '-' then '0' else 配当 end) as 税引前配当額
                    from nikkei_data
                    left join stock_data on nikkei_data.コード == stock_data.銘柄コード
                    where コード == '{select_code[:4]}' 
                """
                df_ideal = pd.read_sql_query(sql = query, con = conn)
                df_ideal = df_ideal.rename(columns={'コード':'code', '銘柄名':'brand'})
                df_ideal.to_sql('ideal_portfolio', conn, if_exists='replace',index=None)
                
                # カラーリストの取得
                color_list = create_color_list()
                color_list = create_color_list(color_list=color_list,color_index=len(color_list),max_num=225,min_num=25,d_num=100)
                
                # sqlにcolor_ideal_portfolioテーブルを追加もしくは置換
                query = """
                    select code, brand from ideal_portfolio
                    group by brand
                    order by brand
                """
                df_color = pd.read_sql_query(sql = query, con = conn)
                df_color['brand_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['brand']))]  
                # sqlにcolor_ideal_portfolioテーブルを追加もしくは置換
                df_color.to_sql('color_ideal_portfolio', conn, if_exists='replace',index=None)
    
            else:
                query = f"""
                    select コード, 銘柄名, 1 as posession_num, (case 終値 when ' ' then '0.0001' else 終値 end) as valuation_yen, industry17, industry33,
                    (case 終値 when ' ' then '0.0001' else 終値 end) as 株価, (case 配当 when '-' then '0' else 配当 end) as 配当, (case 配当 when '-' then '0' else 配当 end) as 税引前配当額
                    from nikkei_data
                    left join stock_data on nikkei_data.コード == stock_data.銘柄コード
                    where コード == '{select_code[:4]}' 
                """
                df_ideal = pd.read_sql_query(sql = query, con = conn)
                df_ideal = df_ideal.rename(columns={'コード':'code', '銘柄名':'brand'})
                for i in range(len(df_ideal['code'])):
                    query = """
                        insert into ideal_portfolio (code,brand,posession_num,valuation_yen,industry17,industry33, 株価, 配当, 税引前配当額)
                        values (?,?,?,?,?,?,?,?,?)
                    """
                    data = [df_ideal['code'][i], df_ideal['brand'][i], int(df_ideal['posession_num'][i]), df_ideal['valuation_yen'][i], df_ideal['industry17'][i], df_ideal['industry33'][i], df_ideal['株価'][i],  df_ideal['配当'][i], df_ideal['税引前配当額'][i]]
                    # データ注入
                    cur.execute(query,data)
                    # コミット
                    conn.commit()
                    
                # カラーリストの取得
                color_list = create_color_list()
                color_list = create_color_list(color_list=color_list,color_index=len(color_list),max_num=225,min_num=25,d_num=100)
                
                # sqlにcolor_ideal_portfolioテーブルを追加もしくは置換
                query = """
                    select code, brand from ideal_portfolio
                    group by brand
                    order by brand
                """
                df_color = pd.read_sql_query(sql = query, con = conn)
                df_color['brand_color'] = [color_list[i%len(color_list)] for i in range(len(df_color['brand']))]  
                # sqlにcolor_ideal_portfolioテーブルを追加もしくは置換
                df_color.to_sql('color_ideal_portfolio', conn, if_exists='replace',index=None)
                
            st.info('登録完了')
    return


# =============================================================================
# データ取り出し用文の作成
# =============================================================================
def create_sql_doc(doc_list=[],add=''):
    result = ''
    if len(doc_list)==0: return
    for i in range(len(doc_list)):
        result += add+doc_list[i]+add
        if i!=len(doc_list)-1:
            result += ','
    return result


# =============================================================================
# グラフの表示
# =============================================================================
# 円グラフ（2重）と表
def plot_2piegraph_table(title, df_data, data, df_target, target, df_color, label=True, linewidth=1):
    matplotlib.use("agg")
    _lock = RendererAgg.lock
    
    # ポートフォリオデータのラベル作成（最大7文字）（割合が5%未満は重複防止のため改行を追加）
    data_label = []
    for i in range(len(df_data['銘柄名'])):
        # if df_data['資産割合(%)'][i]<5: data_label.append('\n'+df_data['銘柄名'][i][:7])
        # else: data_label.append(df_data['銘柄名'][i][:7])
        data_label.append(df_data['銘柄名'][i][:7])
        
    # targetのラベル作成（最大7文字）（割合が5%未満は重複防止のため改行を追加）
    target_label = []
    for i in range(len(df_target[target])): target_label.append(df_target[target][i][:7])
    
    # カラーの追加 
    data_color = [df_color[i] for i in df_data['銘柄名']]
        
    # target別のカラーを設定
    target_color = [df_color[i] for i in df_target[target]]
    
    # # 画面を横に区切ってグラフと表を表示
    row0_spacer1,row0_1,row0_spacer2,row0_2,row0_spacer3 = st.columns((0.2,3,0.2,3,0.2))
    with row0_1,_lock:
        # 外側の円グラフ作成（銘柄別）
        # st.header(title)
        fig, ax = plt.subplots()
        ax.pie(df_data[data],labeldistance=0.7,counterclock=False,startangle=90,colors=data_color,wedgeprops={'width':0.3,'linewidth' : f'{linewidth}', 'edgecolor' : 'white'})
        # 凡例の設定
        ax.legend(data_label,bbox_to_anchor=(0.9, 0.9), fontsize="x-small")
        
        # 内側の円グラフ作成（target別）
        if label: ax.pie(df_target[data],labels=target_label,rotatelabels=True,labeldistance=0.2,counterclock=False,pctdistance=0.8,startangle=90,radius=0.7,colors=target_color,wedgeprops={ 'linewidth' : f'{linewidth}', 'edgecolor' : 'white'})
        else: ax.pie(df_target[data],counterclock=False,pctdistance=0.8,startangle=90,radius=0.7,colors=target_color,wedgeprops={ 'linewidth' : f'{linewidth}', 'edgecolor' : 'white'})
        # p = plt.gcf()
        # p.gca().add_artist(plt.Circle( (0,0), 0.7, color='white'))
        # グラフの表示
        st.pyplot(fig)
        
    with row0_2:
        df_plot_table = df_data.loc[:,[target,'銘柄名',data,'資産割合(%)']]
        df_plot_table = df_plot_table.append(df_plot_table.sum(numeric_only=True), ignore_index=True)
        df_plot_table.loc[len(df_plot_table)-1,'銘柄名'] = ["合計"]
        df_plot_table = df_plot_table.style.format(formatter={(data):"{:,.1f}",('資産割合(%)'):"{:.1f}"},na_rep="-")
        st.dataframe(df_plot_table,height=370)
    
    return


# 円グラフ（１重）と表
def plot_1piegraph_table(title,df_data,data,target,df_color, linewidth=0.4):
    matplotlib.use("agg")
    _lock = RendererAgg.lock
    
    # ポートフォリオデータのラベル作成（最大7文字）（割合が5%未満は重複防止のため改行を追加）
    data_label = []
    for i in range(len(df_data[target])):
        # if df_data['資産割合(%)'][i]<5: data_label.append('\n'+df_data['銘柄名'][i][:7])
        # else: data_label.append(df_data['銘柄名'][i][:7])
        data_label.append(df_data[target][i][:7])
        
    # カラーの追加 
    data_color = [df_color[i] for i in df_data[target]]
    
    # # 画面を横に区切ってグラフと表を表示
    row0_spacer1,row0_1,row0_spacer2,row0_2,row0_spacer3 = st.columns((0.2,3,0.2,3,0.2))
    with row0_1,_lock:
        # 外側の円グラフ作成（銘柄別）
        # st.header(title)
        fig, ax = plt.subplots()
        ax.pie(df_data[data], counterclock=False, startangle=90, colors=data_color, wedgeprops={'linewidth' : f'{linewidth}', 'edgecolor' : 'white'})
        # 凡例の設定
        ax.legend(data_label,bbox_to_anchor=(0.9, 0.9), fontsize="small")
        # グラフの表示
        st.pyplot(fig)
        
    with row0_2:
        df_plot_table = df_data.loc[:,[target,data,'資産割合(%)']]
        df_plot_table = df_plot_table.append(df_plot_table.sum(numeric_only=True), ignore_index=True)
        df_plot_table.loc[len(df_plot_table)-1,target] = ["合計"]
        df_plot_table = df_plot_table.style.format(formatter={(data):"{:,.1f}",('資産割合(%)'):"{:.2f}"},na_rep="-")
        st.dataframe(df_plot_table,height=370)
    
    return


# 棒グラフと表
def plot_bargraph_table(title,df_target,target,margin=0.4):
    matplotlib.use("agg")
    _lock = RendererAgg.lock
    
    
    # 画面を横に区切ってグラフと表を表示
    row0_spacer1,row0_1,row0_spacer2,row0_2,row0_spacer3 = st.columns((0.2,3,0.2,3,0.2))
    with row0_1,_lock:
        # 表のサイズ設定（縦方向範囲3～15）
        fig, ax = plt.subplots(figsize=(5,min(10,max(3,len(df_target)))))
        # ラベル作成(最大７文字)
        target_legend = ['取得総額','評価額']
        target_label = []
        for i in range(len(df_target[target])): target_label.append(df_target[target][i][:7])
        # プロットする値の抜出
        df_plot = [list(df_target['取得総額(円)']),list(df_target['評価額(円)'])]
        # マージンを設定
        # margin = 0.4  #0 <margin< 1
        total_width = 1 - margin
        
        # # 棒グラフの作成（縦）
        # x = np.array([i for i in range(1,len(df_target)+1) ])
        # # マージンを設定
        # # margin = 0.4  #0 <margin< 1
        # total_width = 1 - margin
        # 
        # for i,h in enumerate(df_plot):
        #     pos = x-total_width*(1-(2*i+1)/len(df_plot))/2
        #     ax.bar(pos,h,width=total_width/len(df_plot),edgecolor="#000000", linewidth=1)
        # # y軸ラベルの表示
        # y_label = '金額(円)'
        # ax.set_ylabel(y_label)
        # # ラベルの設定
        # ax.set_xticks(x)
        # ax.set_xticklabels(target_label)
        # # 凡例の設定
        # ax.legend(target_legend)
        # # 補助線の設定
        # ax.minorticks_on()
        # ax.grid(which="major",axis="y",color = "gray", linestyle="-")
        # ax.grid(which="minor",axis="y",color = "gray", linestyle=":")
        
        # 棒グラフの作成（横）
        x = np.array([i for i in range(1,len(df_target)+1) ])
        # 一次元配列xを反転
        x = np.flipud(x)
        for i,h in enumerate(df_plot):
            pos = x-total_width*((2*i+1)/len(df_plot)-1)/2
            ax.barh(pos,h,height=total_width/len(df_plot),edgecolor="#000000", linewidth=1)
        # x軸ラベルの表示
        x_label = '金額(円)'
        ax.set_xlabel(x_label)
        # ラベルの設定
        ax.set_yticks(x)
        ax.set_yticklabels(target_label)
        # 凡例の設定
        ax.legend(target_legend)
        # 補助線の設定
        ax.minorticks_on()
        ax.grid(which="major",axis="x",color = "gray", linestyle="-")
        ax.grid(which="minor",axis="x",color = "gray", linestyle=":")
        
        # グラフの表示
        st.pyplot(fig)
    
    with row0_2:
        df_plot_table = df_target.loc[:,[target,'取得総額(円)','評価額(円)','損益(円)']]
        df_plot_table['取得額割合(%)'] = 100*df_plot_table['取得総額(円)']/df_plot_table['取得総額(円)'].sum()
        df_plot_table['評価額割合(%)'] = 100*df_plot_table['評価額(円)']/df_plot_table['評価額(円)'].sum()
        # 数値項目の合計を計算
        df_plot_table = df_plot_table.append(df_plot_table.sum(numeric_only=True), ignore_index=True)
        df_plot_table.loc[len(df_plot_table)-1,target] = ["合計"]
        df_plot_table['損益(%)'] = (df_plot_table['評価額(円)']-df_plot_table['取得総額(円)'])/df_plot_table['取得総額(円)']*100
        df_plot_table = df_plot_table.reindex(columns=[target,'取得総額(円)','評価額(円)','損益(円)','損益(%)','取得額割合(%)','評価額割合(%)'])
        df_plot_table = df_plot_table.style.format(formatter={('取得総額(円)'):"{:,.1f}",('評価額(円)'):"{:,.1f}",('損益(円)'):"{:,.1f}",('損益(%)'):"{:.2f}",('取得額割合(%)'):"{:.2f}",('評価額割合(%)'):"{:.2f}"},na_rep="-")
        st.dataframe(df_plot_table,height=400)
    return


# 棒グラフと表2
def plot_bargraph_table2(title,df_target,target,margin=0.4):
    matplotlib.use("agg")
    _lock = RendererAgg.lock
    
    
    # 画面を横に区切ってグラフと表を表示
    row0_spacer1,row0_1,row0_spacer2,row0_2,row0_spacer3 = st.columns((0.2,3,0.2,3,0.2))
    with row0_1,_lock:
        # 表のサイズ設定（縦方向範囲3～10）
        fig, ax = plt.subplots(figsize=(5,min(10,max(3,len(df_target)))))
        # ラベル作成(最大７文字)
        # target_legend = ['取得総額','評価額']
        target_label = []
        for i in range(len(df_target[target])): target_label.append(df_target[target][i][:7])
        # プロットする値の抜出
        df_plot = [list(df_target['評価額(円)'])]
        # マージンを設定
        # margin = 0.4  #0 <margin< 1
        total_width = 1 - margin
        
        # # 棒グラフの作成（縦）
        # x = np.array([i for i in range(1,len(df_target)+1) ])
        # # マージンを設定
        # # margin = 0.4  #0 <margin< 1
        # total_width = 1 - margin
        # 
        # for i,h in enumerate(df_plot):
        #     pos = x-total_width*(1-(2*i+1)/len(df_plot))/2
        #     ax.bar(pos,h,width=total_width/len(df_plot),edgecolor="#000000", linewidth=1)
        # # y軸ラベルの表示
        # y_label = '金額(円)'
        # ax.set_ylabel(y_label)
        # # ラベルの設定
        # ax.set_xticks(x)
        # ax.set_xticklabels(target_label)
        # # 凡例の設定
        # ax.legend(target_legend)
        # # 補助線の設定
        # ax.minorticks_on()
        # ax.grid(which="major",axis="y",color = "gray", linestyle="-")
        # ax.grid(which="minor",axis="y",color = "gray", linestyle=":")
        
        # 棒グラフの作成（横）
        x = np.array([i for i in range(1,len(df_target)+1) ])
        # 一次元配列xを反転
        x = np.flipud(x)
        for i,h in enumerate(df_plot):
            pos = x-total_width*((2*i+1)/len(df_plot)-1)/2
            ax.barh(pos,h,height=total_width/len(df_plot),edgecolor="#000000", linewidth=1)
        # x軸ラベルの表示
        x_label = '金額(円)'
        ax.set_xlabel(x_label)
        # ラベルの設定
        ax.set_yticks(x)
        ax.set_yticklabels(target_label)
        # # 凡例の設定
        # ax.legend(target_legend)
        # 補助線の設定
        ax.minorticks_on()
        ax.grid(which="major",axis="x",color = "gray", linestyle="-")
        ax.grid(which="minor",axis="x",color = "gray", linestyle=":")
        
        # グラフの表示
        st.pyplot(fig)
    
    with row0_2:
        df_plot_table = df_target.loc[:,[target,'数量(株)','評価額(円)']]
        df_plot_table['評価額割合(%)'] = 100*df_plot_table['評価額(円)']/df_plot_table['評価額(円)'].sum()
        # 数値項目の合計を計算
        df_plot_table = df_plot_table.append(df_plot_table.sum(numeric_only=True), ignore_index=True)
        df_plot_table.loc[len(df_plot_table)-1,target] = ["合計"]
        df_plot_table = df_plot_table.reindex(columns=[target,'評価額(円)','評価額割合(%)'])
        df_plot_table = df_plot_table.style.format(formatter={('評価額(円)'):"{:,.1f}",('評価額割合(%)'):"{:.2f}"},na_rep="-")
        st.dataframe(df_plot_table,height=400)
    return


# =============================================================================
# 起動
# =============================================================================
if __name__ == '__main__':
    main()