import pandas as pd
from typing import Optional
import os
from datetime import datetime

def load_location_data(file_path: str) -> Optional[pd.DataFrame]:
    """
    部門コード_場所.csvを読み込み、場所データをDataFrameとして返します。

    Args:
        file_path (str): CSVファイルのパス。

    Returns:
        Optional[pd.DataFrame]: 部門コード、場所を含むDataFrame。エラーの場合はNone。
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig', dtype={'部門コード': str})
        
        required_columns = ['部門コード', '部門名', '場所']
        if not set(required_columns).issubset(df.columns):
            # cp932で再試行
            try:
                df = pd.read_csv(file_path, encoding='cp932', dtype={'部門コード': str})
                if not set(required_columns).issubset(df.columns):
                    raise ValueError("必須カラムが見つかりません。")
            except Exception:
                missing = set(required_columns) - set(df.columns)
                print(f"エラー: 必須カラム {missing} が '{file_path}' に見つかりません。")
                return None
        
        # 必要なカラムのみを返す
        return df[required_columns]

    except FileNotFoundError:
        print(f"エラー: 場所ファイルが見つかりません - {file_path}")
        return None
    except Exception as e:
        print(f"予期せぬエラーが発生しました ({file_path}読み込み中): {e}")
        return None

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

    csv_file = '部門コード_場所.csv'
    
    print(f"--- '{csv_file}' の処理を開始します ---")
    location_df = load_location_data(csv_file)
    print("------------------------------------")

    if location_df is not None:
        print("\n--- 読み込み成功: 場所データ（先頭5行） ---")
        print(location_df.head())
        print("\n--- カラム一覧 ---")
        print(location_df.columns.tolist())
        print("\n--- データ型 ---")
        print(location_df.dtypes)
        save_df_to_debug(location_df, 'module_location_result')
    else:
        print(f"\n'{csv_file}' の処理に失敗しました。") 