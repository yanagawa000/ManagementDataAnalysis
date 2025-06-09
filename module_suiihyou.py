'''推移表の前処理モジュール'''
import pandas as pd
from typing import Tuple

def preprocess_csv(file_path: str, skip_rows: int = 9) -> Tuple[pd.DataFrame, float]:
    """CSVファイルを読み込み、前処理を行います。BS1043の合計値を計算し、指定の勘定科目を削除します。

    Args:
        file_path (str): CSVファイルのパス。
        skip_rows (int, optional): スキップする行数。デフォルトは9。

    Returns:
        Tuple[pd.DataFrame, float]: 
            前処理後の縦持ちDataFrameと、'BS1043'の合計金額のタプル。
    """
    try:
        df = pd.read_csv(file_path, skiprows=skip_rows, encoding='cp932')

        if df.empty:
            print(f"警告: ファイル '{file_path}' から読み込んだデータが空です。")
            return pd.DataFrame(), 0.0

        # 「勘定科目コード」列の存在確認とフィルタリング
        if '勘定科目コード' in df.columns:
            df = df[df['勘定科目コード'].astype(str).str.startswith('BS', na=False)]
        else:
            print(f"警告: '勘定科目コード' 列がファイル '{file_path}' に見つかりません。BSフィルタリングはスキップされました。")

        if df.empty:
            print(f"警告: BSフィルタリング後、データが空になりました。ファイル: '{file_path}'")
            return pd.DataFrame(), 0.0

        # 縦持ち変換のID列を指定
        id_columns = ['部門コード', '部門名', '勘定科目コード', '勘定科目名']
        actual_id_columns = [col for col in id_columns if col in df.columns]

        if not actual_id_columns:
            print(f"警告: ID列が見つかりません。縦持ち変換はスキップされました。")
            return df, 0.0
        
        value_columns = [col for col in df.columns if col not in actual_id_columns]

        if not value_columns:
            print(f"警告: ID列以外のデータ列が見つかりません。縦持ち変換はスキップされました。")
            return df, 0.0

        df_melted = df.melt(
            id_vars=actual_id_columns,
            value_vars=value_columns,
            var_name='対象月',
            value_name='金額'
        )

        if df_melted.empty:
            print(f"警告: 縦持ち変換後、データが空になりました。")
            return df_melted, 0.0

        # --- 追加処理 ---
        # 1. 金額を数値に変換
        df_melted['金額'] = pd.to_numeric(df_melted['金額'], errors='coerce').fillna(0)
        
        # 2. BS1043の合計金額を計算
        sum_bs1043 = df_melted[df_melted['勘定科目コード'] == 'BS1043']['金額'].sum()
        print(f"情報: 'BS1043' の合計金額を計算しました: {sum_bs1043:,.0f}")

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
            
            if df_melted.empty:
                print(f"警告: 「前残」レコード削除後、データが空になりました。")
                return df_melted, sum_bs1043

            df_melted['対象月'] = df_melted['対象月'].astype(str).str.replace('月度', '', regex=False)
            df_melted['対象月'] = df_melted['対象月'].str.replace('年', '/', regex=False) + '/01'
            df_melted.rename(columns={'対象月': '日付'}, inplace=True)
        
        desired_order = ['日付', '部門コード', '部門名', '勘定科目コード', '勘定科目名', '金額']
        final_columns = [col for col in desired_order if col in df_melted.columns]
        df_processed = df_melted[final_columns]

        return df_processed, sum_bs1043

    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません - {file_path}")
        return pd.DataFrame(), 0.0
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return pd.DataFrame(), 0.0

if __name__ == '__main__':
    csv_file = '2024年8月度データ/推移表_貸借対照表_部門別_2024_上期.csv'
    processed_data, sum_value = preprocess_csv(csv_file)

    if not processed_data.empty:
        print("\n--- 処理後のDataFrame（先頭20行） ---")
        print(processed_data.head(20))
        print("\n--- 処理後のDataFrame（末尾20行） ---")
        print(processed_data.tail(20))
        print(f"\nカラム一覧: {processed_data.columns.tolist()}")
    else:
        print(f"'{csv_file}' の処理結果が空です。")
        
    print("\n--- BS1043の合計金額 ---")
    print(f"{sum_value:,.0f}")
