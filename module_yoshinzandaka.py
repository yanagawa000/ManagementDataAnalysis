'''与信残高表データの前処理モジュール'''
import pandas as pd
from typing import Optional, Any, Tuple
import os
from datetime import datetime

def get_first_lines(file_path: str, num_lines: int) -> Tuple[list[str], str]:
    """ファイルの先頭から指定された行数を読み込み、エンコーディングを自動判別します。"""
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            lines = [f.readline() for _ in range(num_lines)]
        return lines, 'utf-8-sig'
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='cp932') as f:
            lines = [f.readline() for _ in range(num_lines)]
        return lines, 'cp932'

def preprocess_yoshin(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[Any]]:
    """
    与信残高表CSVを前処理します。

    処理フロー:
    1. ファイルの先頭13行をテキストとして読み込み、5行2列目の日付情報を取得します。
    2. 1-13行目のメタ情報をスキップしてデータをPandasで読み込みます。
    3. '計上部門コード'が空の行を削除し、必要なカラムを抽出・リネームします。
    4. 新しいカラム('日付', '勘定科目コード', '勘定科目名')を追加します。
    5. 最終的なカラム順序に並べ替えます。

    Args:
        file_path (str): CSVファイルのパス。

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[Any]]:
            処理後のDataFrameと、取得した日付情報のタプル。
            エラーの場合は (None, None)。
    """
    try:
        # --- 1. メタ情報部分をテキストとして読み込み、特定の値を取得 ---
        meta_lines, encoding = get_first_lines(file_path, 13)
        
        date_value_str = None
        date_obj = None
        if len(meta_lines) >= 5:
            columns = meta_lines[4].strip().split(',')
            if len(columns) >= 2:
                # 2列目の値を取得し、余分な空白や引用符を削除
                date_value_str = columns[1].strip().strip('"')
                try:
                    # 日付文字列をdatetimeオブジェクトに変換
                    date_obj = pd.to_datetime(date_value_str)
                    print(f"情報: 5行2列目の日付を取得・解析しました: '{date_value_str}' -> {date_obj.strftime('%Y-%m-%d')}")
                except (ValueError, TypeError):
                    date_obj = None # 解析失敗時はNoneを維持
                    print(f"警告: メタ情報 '{date_value_str}' を日付として解析できませんでした。")
            else:
                 print("警告: 5行目に2つ以上の列がありませんでした。")
        else:
            print("警告: ファイルに5行分のデータがありませんでした。")

        # --- 2. 1-13行目をスキップして本データを読み込み ---
        df = pd.read_csv(file_path, skiprows=13, encoding=encoding, engine='python')

        # --- 3. 必要なカラムを抽出し、フィルタリング ---
        required_columns = ['計上部門コード', '計上部門名', '受取手形']
        if not set(required_columns).issubset(df.columns):
            missing = set(required_columns) - set(df.columns)
            print(f"エラー: 必須カラム {missing} が見つかりません。処理を中断します。")
            return None, date_value_str
        
        df_work = df[required_columns].copy()
        df_work.dropna(subset=['計上部門コード'], inplace=True)

        # --- 4. カラム名の変更と追加 ---
        df_work.rename(columns={
            '計上部門コード': '部門コード',
            '計上部門名': '部門名',
            '受取手形': '金額'
        }, inplace=True)
        
        df_work['日付'] = date_obj
        df_work['勘定科目コード'] = 'BS1013'
        df_work['勘定科目名'] = '★受取手形'

        # --- 5. カラムの順序を整理 ---
        final_order = ['日付', '部門コード', '部門名', '勘定科目コード', '勘定科目名', '金額']
        final_columns = [col for col in final_order if col in df_work.columns]
        df_final = df_work[final_columns]

        return df_final, date_value_str

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
    test_yoshin_path = '2024年8月度データ/与信残高表_202408.csv'

    print(f"--- '{test_yoshin_path}' の処理を開始します ---")
    df, date = preprocess_yoshin(test_yoshin_path)
    print("---------------------------------------------")

    if df is not None:
        print("\n--- 処理成功: 与信残高表データ（先頭5行）---")
        print(df.head())
        print(f"\n取得した日付: {date}")
        print("\n--- データ型 ---")
        print(df.dtypes)
        save_df_to_debug(df, 'module_yoshinzandaka_result')
    else:
        print(f"--- '{test_yoshin_path}' の処理に失敗しました ---")
