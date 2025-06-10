'''MAC元帳データの前処理モジュール'''
import pandas as pd
from typing import Optional, Tuple
import os
from datetime import datetime

def _get_latest_record(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """
    DataFrameから計上日が最新で、行の値が最大のレコードを1つ抽出します。
    """
    if df.empty:
        print(f"情報: '{code}' のデータは空のため、抽出処理をスキップします。")
        return df
    
    # データ型を変換（変換エラーは無視）
    df['計上日'] = pd.to_datetime(df['計上日'], errors='coerce')
    df['行'] = pd.to_numeric(df['行'], errors='coerce')

    # 変換に失敗した行や、値がない行を削除
    df.dropna(subset=['計上日', '行'], inplace=True)

    if df.empty:
        print(f"警告: '{code}' のデータは日付または行の変換に失敗したため、結果が空になりました。")
        return df

    # 計上日と行でソートし、最新・最大のレコードを取得
    latest_record_df = df.sort_values(by=['計上日', '行'], ascending=[False, False]).head(1)
    print(f"情報: '{code}' のデータから最新・最大のレコードを1件抽出しました。")
    return latest_record_df

def preprocess_motocho(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    MAC元帳CSVを読み込み、勘定科目コードごとに最新レコードを抽出し、整形したDataFrameを返します。

    処理フロー:
    1. CSVファイルを読み込みます。
    2. 必要な9つのカラムを抽出します。
    3. '勘定科目コード'でデータを'109801'と'491701'に分割します。
    4. 各DataFrameから「計上日が最新」かつ「行が最大」のレコードを1つずつ抽出します。
    5. 各DataFrameから不要なカラムを削除します。
    6. 各DataFrameのカラム名を標準形式にリネームします。
    7. 各DataFrameの値を指定された内容に更新・計算（日付を月初に変換するなど）します。

    Args:
        file_path (str): CSVファイルのパス。

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]: 
            処理済みの'109801'のDataFrameと'491701'のDataFrameのタプル。
            エラーの場合は(None, None)。
    """
    try:
        # --- 1. CSVファイルの読み込み ---
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig', dtype={'勘定科目コード': str, '行': str})
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='cp932', dtype={'勘定科目コード': str, '行': str})
        print(f"情報: '{file_path}' を正常に読み込みました。")

        # --- 2. 必要なカラムの抽出 ---
        required_columns = [
            'データ区分', '計上日', '行', '部門コード', '部門名', 
            '勘定科目コード', '勘定科目', '借方金額', '残高'
        ]
        if not set(required_columns).issubset(df.columns):
            missing = set(required_columns) - set(df.columns)
            print(f"エラー: 必須カラム {missing} が見つかりません。処理を中断します。")
            return None, None
        
        df_extracted = df[required_columns].copy()
        print(f"情報: {len(required_columns)}個のカラムを抽出しました。")

        # --- 3. '勘定科目コード' ごとにDataFrameを分割 ---
        print("情報: '勘定科目コード' ごとにデータを分割します。")
        
        df_109801 = df_extracted[df_extracted['勘定科目コード'] == '109801'].copy()
        print(f"  - '109801' のデータ: {len(df_109801)}行")
        
        df_491701 = df_extracted[df_extracted['勘定科目コード'] == '491701'].copy()
        print(f"  - '491701' のデータ: {len(df_491701)}行")

        # --- 4. 各DataFrameから最新・最大の行を抽出 ---
        df_109801_final = _get_latest_record(df_109801, '109801')
        df_491701_final = _get_latest_record(df_491701, '491701')

        # --- 5. DataFrameごとに不要なカラムを削除 ---
        print("情報: DataFrameごとに不要なカラムを削除します。")
        if not df_109801_final.empty:
            cols_to_drop_1 = ['借方金額', 'データ区分', '行']
            df_109801_final.drop(columns=cols_to_drop_1, inplace=True, errors='ignore')
            print(f"  - '109801'から {cols_to_drop_1} を削除。")

        if not df_491701_final.empty:
            cols_to_drop_2 = ['残高', 'データ区分', '行']
            df_491701_final.drop(columns=cols_to_drop_2, inplace=True, errors='ignore')
            print(f"  - '491701'から {cols_to_drop_2} を削除。")

        # --- 6. カラム名を標準形式にリネーム ---
        print("情報: カラム名を標準形式にリネームします。")
        if not df_109801_final.empty:
            df_109801_final.rename(columns={'計上日': '日付', '勘定科目': '勘定科目名', '残高': '金額'}, inplace=True)
            print("  - '109801'のカラム名をリネームしました。")
        
        if not df_491701_final.empty:
            df_491701_final.rename(columns={'計上日': '日付', '勘定科目': '勘定科目名', '借方金額': '金額'}, inplace=True)
            print("  - '491701'のカラム名をリネームしました。")

        # --- 7. 値の更新と計算 ---
        print("情報: DataFrameごとに値を更新・計算します。")
        if not df_109801_final.empty:
            # 日付を月初に変換
            df_109801_final['日付'] = df_109801_final['日付'].dt.to_period('M').dt.to_timestamp()
            # カンマを削除してから数値に変換
            df_109801_final['金額'] = pd.to_numeric(
                df_109801_final['金額'].astype(str).str.replace(',', ''), 
                errors='coerce'
            ).fillna(0)
            df_109801_final.loc[:, '部門コード'] = 'H101210'
            df_109801_final.loc[:, '部門名'] = '原料部_輸'
            df_109801_final.loc[:, '勘定科目コード'] = 'BS1098'
            df_109801_final.loc[:, '勘定科目名'] = '★輸入消費税'
            print("  - '109801'の各カラムに固定値を設定しました。")

        if not df_491701_final.empty:
            # 日付を月初に変換
            df_491701_final['日付'] = df_491701_final['日付'].dt.to_period('M').dt.to_timestamp()
            # カンマを削除してから数値に変換
            df_491701_final['金額'] = pd.to_numeric(
                df_491701_final['金額'].astype(str).str.replace(',', ''),
                errors='coerce'
            ).fillna(0)
            df_491701_final.loc[:, '勘定科目コード'] = 'BS1043'
            df_491701_final.loc[:, '勘定科目名'] = '★商品'
            df_491701_final.loc[:, '金額'] *= -1
            print("  - '491701'の値を更新し、金額に-1を乗算しました。")

        return df_109801_final, df_491701_final

    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません - {file_path}")
        return None, None
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        return None, None


if __name__ == '__main__':
    # モジュールのテスト用コード
    # --- デバッグ用セットアップ ---
    DEBUG_DIR = 'dbug'
    if not os.path.exists(DEBUG_DIR):
        os.makedirs(DEBUG_DIR)

    def save_df_to_debug(df: pd.DataFrame, name: str):
        """DataFrameをdbugフォルダに保存する。"""
        if isinstance(df, pd.DataFrame) and not df.empty:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{name}.csv"
            path = os.path.join(DEBUG_DIR, filename)
            try:
                df.to_csv(path, index=False, encoding='utf-8-sig')
                print(f"[DEBUG] DataFrame '{name}' を '{path}' に保存しました。")
            except Exception as e:
                print(f"[DEBUG] DataFrame '{name}' の保存中にエラー: {e}")

    # テスト用のファイルパス
    test_motocho_path = '2024年8月度データ/MAC元帳202408.csv'
    
    print(f"--- '{test_motocho_path}' の処理を開始します ---")
    df1, df2 = preprocess_motocho(test_motocho_path)
    print("------------------------------------------")

    if df1 is not None:
        print("\n--- 処理成功: df_motocho_1 (109801) ---")
        print(df1)
        save_df_to_debug(df1, 'module_motocho_109801_result')
    else:
        print("\n--- df_motocho_1 (109801) の処理に失敗、またはデータなし ---")

    if df2 is not None:
        print("\n--- 処理成功: df_motocho_2 (491701) ---")
        print(df2)
        save_df_to_debug(df2, 'module_motocho_491701_result')
    else:
        print("\n--- df_motocho_2 (491701) の処理に失敗、またはデータなし ---")
