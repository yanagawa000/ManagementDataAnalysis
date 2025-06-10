'''推移表の前処理モジュール'''
import pandas as pd
from typing import Tuple, Optional
import os
from datetime import datetime

def preprocess_csv(file_path: str, skip_rows: int = 9) -> Tuple[Optional[pd.DataFrame], pd.Series]:
    """CSVファイルを読み込み、前処理を行います。BS1043の日付ごと合計値を計算し、指定の勘定科目を削除します。

    Args:
        file_path (str): CSVファイルのパス。
        skip_rows (int, optional): スキップする行数。デフォルトは9。

    Returns:
        Tuple[Optional[pd.DataFrame], pd.Series]: 
            前処理後の縦持ちDataFrameと、'BS1043'の日付ごと合計金額を持つSeriesのタプル。
            エラー時は (None, 空のSeries)。
    """
    try:
        df = pd.read_csv(file_path, skiprows=skip_rows, encoding='cp932')

        if df.empty:
            print(f"警告: ファイル '{file_path}' から読み込んだデータが空です。")
            return None, pd.Series(dtype='float64')

        # 「勘定科目コード」列の存在確認とフィルタリング
        if '勘定科目コード' in df.columns:
            df = df[df['勘定科目コード'].astype(str).str.startswith('BS', na=False)]
        else:
            print(f"警告: '勘定科目コード' 列がファイル '{file_path}' に見つかりません。BSフィルタリングはスキップされました。")
            return None, pd.Series(dtype='float64')

        if df.empty:
            print(f"警告: BSフィルタリング後、データが空になりました。ファイル: '{file_path}'")
            return None, pd.Series(dtype='float64')

        # 縦持ち変換のID列を指定
        id_columns = ['部門コード', '部門名', '勘定科目コード', '勘定科目名']
        actual_id_columns = [col for col in id_columns if col in df.columns]

        if not actual_id_columns:
            print(f"警告: ID列が見つかりません。縦持ち変換はスキップされました。")
            return df, pd.Series(dtype='float64')
        
        value_columns = [col for col in df.columns if col not in actual_id_columns]

        if not value_columns:
            print(f"警告: ID列以外のデータ列が見つかりません。縦持ち変換はスキップされました。")
            return df, pd.Series(dtype='float64')

        df_melted = df.melt(
            id_vars=actual_id_columns,
            value_vars=value_columns,
            var_name='対象月',
            value_name='金額'
        )

        if df_melted.empty:
            print(f"警告: 縦持ち変換後、データが空になりました。")
            return df_melted, pd.Series(dtype='float64')

        # --- 追加処理 ---
        # 1. 金額を数値に変換
        df_melted['金額'] = pd.to_numeric(df_melted['金額'], errors='coerce').fillna(0)
        
        # 2. BS1043の日付ごと合計金額を計算
        #    日付変換は後続処理で行うため、一旦「対象月」で集計
        sum_bs1043_by_month = df_melted[df_melted['勘定科目コード'] == 'BS1043'].groupby('対象月')['金額'].sum()

        # 3. 指定された勘定科目コードのレコードを削除
        codes_to_delete = ['BS1043', 'BS1098', 'BS2003', 'BS1013', 'BS1015', 'BS2004']
        original_rows = len(df_melted)
        df_melted = df_melted[~df_melted['勘定科目コード'].isin(codes_to_delete)]
        print(f"情報: 指定された勘定科目コードのレコードを{original_rows - len(df_melted)}行削除しました。")
        # --- 追加処理ここまで ---

        # 「対象月」列の処理
        if '対象月' in df_melted.columns:
            # 「前残」レコードを削除
            df_melted = df_melted[df_melted['対象月'] != '前残']
            sum_bs1043_by_month = sum_bs1043_by_month[sum_bs1043_by_month.index != '前残']
            
            if df_melted.empty:
                print(f"警告: 「前残」レコード削除後、データが空になりました。")
                return df_melted, pd.Series(dtype='float64')

            df_melted['対象月'] = df_melted['対象月'].astype(str).str.replace('月度', '', regex=False)
            df_melted['対象月'] = df_melted['対象月'].str.replace('年', '/', regex=False) + '/01'
            df_melted.rename(columns={'対象月': '日付'}, inplace=True)
            df_melted['日付'] = pd.to_datetime(df_melted['日付'], errors='coerce')

            # 集計結果のindexも日付型に変換
            sum_bs1043_by_month.index = pd.to_datetime(
                sum_bs1043_by_month.index.str.replace('月度', '').str.replace('年', '/') + '/01',
                errors='coerce'
            )
            # NaTインデックスを削除
            sum_bs1043_by_month = sum_bs1043_by_month.dropna()

        desired_order = ['日付', '部門コード', '部門名', '勘定科目コード', '勘定科目名', '金額']
        final_columns = [col for col in desired_order if col in df_melted.columns]
        df_processed = df_melted[final_columns]

        return df_processed, sum_bs1043_by_month

    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません - {file_path}")
        return None, pd.Series(dtype='float64')
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return None, pd.Series(dtype='float64')

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
    test_suiihyou_path = '2024年8月度データ/推移表_貸借対照表_部門別_2024_上期.csv'

    print(f"--- '{test_suiihyou_path}' の処理を開始します ---")
    df, sum_series = preprocess_csv(test_suiihyou_path)
    print("---------------------------------------------")

    if df is not None:
        print("\n--- 処理成功: 推移表データ（先頭5行）---")
        print(df.head())
        print("\n--- データ型 ---")
        print(df.dtypes)
        save_df_to_debug(df, 'module_suiihyou_result_df')

        print("\n--- 処理成功: BS1043 合計値シリーズ ---")
        print(sum_series)
        # Seriesもデバッグ出力する場合はDataFrameに変換する
        save_df_to_debug(sum_series.to_frame(name='BS1043_sum'), 'module_suiihyou_result_series')
    else:
        print(f"--- '{test_suiihyou_path}' の処理に失敗しました ---")
