'''MACROSS在庫データの前処理モジュール'''
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple

def preprocess_zaiko_step1(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[datetime]]:
    """在庫一覧CSVを読み込み、一連の前処理を実行して整形されたDataFrameを返します。

    処理フロー:
    1. ファイルの1行目から「基準日」を読み取り、月初の日付を取得します。
    2. 2行目以降のデータを読み込み、必須カラムの存在を確認します。
    3. データを整形します:
        - 必須カラムのみを抽出
        - 部署コードが空の行を削除
        - カラム名を標準形式にリネーム
        - 部署コードに基づき部署名を更新
    4. 最終的な出力形式に必要なカラム（日付、勘定科目コードなど）を追加します。
    5. 指定された順序でカラムを並べ替えたDataFrameを返します。

    Args:
        file_path (str): CSVファイルのパス。

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[datetime]]: 
            処理後のDataFrameと、取得した月初の日付のタプル。
            エラーが発生した場合は (None, None)。
    """
    try:
        # --- 1. 基準日の取得 ---
        try:
            df_first_row = pd.read_csv(file_path, encoding='utf-8-sig', header=None, nrows=1)
        except UnicodeDecodeError:
            df_first_row = pd.read_csv(file_path, encoding='cp932', header=None, nrows=1)

        first_day_of_month = None
        if not df_first_row.empty and '基準日:' in str(df_first_row.iloc[0, 0]):
            date_str = str(df_first_row.iloc[0, 0]).replace('基準日:', '').strip()
            base_date = pd.to_datetime(date_str)
            first_day_of_month = base_date.replace(day=1)
            # print(f"情報: 基準日を処理。月初: {first_day_of_month.strftime('%Y-%m-%d')}")
        else:
            print("警告: 基準日が見つからないか、ファイルの読み込みに失敗しました。")

        # --- 2. 本データの読み込み ---
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig', skiprows=1)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='cp932', skiprows=1)
        df.columns = df.columns.str.replace('\\n', '', regex=False)

        # --- 3. データ整形 ---
        required_columns = ['部署コード', '部署名', '在庫金額']
        if not set(required_columns).issubset(df.columns):
            missing = set(required_columns) - set(df.columns)
            # print(f"エラー: 必須カラム {missing} が見つかりません。処理を中断します。")
            return None, first_day_of_month

        df_work = df[required_columns].dropna(subset=['部署コード']).copy()

        df_work.rename(columns={
            '部署コード': '部門コード',
            '部署名': '部門名',
            '在庫金額': '金額'
        }, inplace=True)

        # 金額カラムを頑健な方法で数値に変換
        # 通貨記号、カンマ、空白などの非数値文字を削除
        cleaned_series = df_work['金額'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        
        # クリーニング後に空文字列になった場合は、数値変換のためにNoneに置換
        cleaned_series.replace('', None, inplace=True)
        
        # 数値に変換し、変換不能な値は0にする
        df_work['金額'] = pd.to_numeric(
            cleaned_series,
            errors='coerce'
        ).fillna(0)

        bumon_map = {'H101210': '原料部_輸', 'H101310': '海外部'}
        df_work['部門名'] = df_work['部門コード'].map(bumon_map).fillna(df_work['部門名'])
        
        # --- 4. カラムの追加と順序設定 ---
        df_work = df_work.assign(
            日付=first_day_of_month,
            勘定科目コード='BS1043',
            勘定科目名='★商品'
        )

        final_order = ['日付', '部門コード', '部門名', '勘定科目コード', '勘定科目名', '金額']
        
        if first_day_of_month is None:
            final_order.remove('日付')
        
        df_final = df_work[final_order]

        # print("情報: データの前処理が完了しました。")
        return df_final, first_day_of_month

    except FileNotFoundError:
        # print(f"エラー: ファイルが見つかりません - {file_path}")
        return None, None
    except Exception as e:
        # print(f"予期せぬエラーが発生しました: {e}")
        return None, None

if __name__ == '__main__':
    # モジュールのテスト用コード
    csv_file = '2024年8月度データ/在庫一覧_202408.csv'
    
    print(f"--- '{csv_file}' の処理を開始します ---")
    processed_data, kijunbi_date = preprocess_zaiko_step1(csv_file)
    print("------------------------------------")

    if processed_data is not None:
        print("\n[処理結果サマリー]")
        if kijunbi_date:
            print(f"  格納された変数（月初の日付）: {kijunbi_date.strftime('%Y-%m-%d')}")
        else:
            print("  月初の日付は取得できませんでした。")
            
        print("\n--- 処理後のDataFrameの先頭20行 ---")
        print(processed_data.head(20))
        
        print("\n--- 処理後のDataFrameのカラム一覧 ---")
        print(processed_data.columns.tolist())
    else:
        print(f"\n'{csv_file}' の処理に失敗しました。DataFrameは生成されませんでした。")
