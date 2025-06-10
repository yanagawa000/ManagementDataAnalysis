'''支払手形データの前処理モジュール'''
import pandas as pd
import os
from datetime import datetime

def preprocess_shiharai_tegata(file_path: str, target_date: pd.Timestamp) -> pd.DataFrame:
    """支払手形CSVファイルを読み込み、列抽出、カラム名変更、固定列追加、列順序変更を行います。

    Args:
        file_path (str): CSVファイルのパス。
        target_date (pd.Timestamp): 処理対象の基準日。

    Returns:
        pd.DataFrame: 処理後のDataFrame。
                     エラーが発生した場合は空のDataFrame。
    """
    try:
        # エンコーディングを utf-8-sig に変更
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 対象カラムのリスト
        required_columns = ['依頼部門コード', '依頼部門名', '手形金額']
        
        # DataFrameに存在する対象カラムのみを抽出
        # 存在しないカラムがあってもエラーとせず、存在するカラムのみで処理を続行
        columns_to_extract = [col for col in required_columns if col in df.columns]
        
        if not columns_to_extract:
            print(f"警告: 指定されたカラム ('{', '.join(required_columns)}') がファイル '{file_path}' に一つも見つかりません。空のDataFrameを返します。")
            return pd.DataFrame()
        elif len(columns_to_extract) < len(required_columns):
            missing_cols = set(required_columns) - set(columns_to_extract)
            print(f"警告: 指定されたカラムのうち、次の列が見つかりません: {missing_cols}。存在するカラムのみ抽出します。")
            
        df_extracted = df[columns_to_extract].copy() # 抽出後にコピーを作成

        # カラム名の変更辞書
        rename_map = {
            '依頼部門コード': '部門コード',
            '依頼部門名': '部門名',
            '手形金額': '金額'
        }
        
        # DataFrameに存在するカラムのみをリネーム対象とする
        actual_rename_map = {old: new for old, new in rename_map.items() if old in df_extracted.columns}
        
        if len(actual_rename_map) < len(columns_to_extract):
            # 抽出はされたがリネーム対象の元の名前ではなかったカラムがある場合
            # 例えば、ユーザーが指定した required_columns の名前がCSVファイル内の実際のヘッダーと少し違ったが、
            # 部分的に一致するなどして抽出自体は成功したが、rename_mapのキーとは一致しないケースを想定。
            # しかし、現状のロジック(columns_to_extractの生成)では、このケースは稀かもしれない。
            # rename_mapのキー(old)は、必ずcolumns_to_extractに含まれているはずなので、
            # この警告が出るのは、主にrename_mapの定義が不完全な場合。
            renamed_cols = set(actual_rename_map.keys())
            expected_to_rename_cols = set(columns_to_extract)
            not_renamed_cols = expected_to_rename_cols - renamed_cols
            if not_renamed_cols:
                 print(f"警告: 抽出されたカラムのうち、次のカラムはリネームマップに定義がありませんでした: {not_renamed_cols}")

        df_extracted.rename(columns=actual_rename_map, inplace=True)
        
        # 新しいカラムの追加
        df_extracted['日付'] = target_date
        df_extracted['勘定科目コード'] = 'BS2003'
        df_extracted['勘定科目名'] = '★支払手形'
        
        # カラムの順序を指定
        desired_order = ['日付', '部門コード', '部門名', '勘定科目コード', '勘定科目名', '金額']
        
        # DataFrameに存在するカラムのみで順序を再構成
        # 意図しないカラムが含まれていたり、不足していてもエラーにならないようにする
        current_columns = df_extracted.columns.tolist()
        final_columns = [col for col in desired_order if col in current_columns] # desired_orderの順序を維持
        # desired_orderに含まれないが、current_columnsには存在する列も末尾に追加する場合（今回は不要と判断）
        # for col in current_columns:
        #     if col not in final_columns:
        #         final_columns.append(col)

        if len(final_columns) != len(desired_order) or set(final_columns) != set(desired_order):
            print(f"警告: 指定されたカラム順序の一部が現在のDataFrameに存在しません。")
            print(f"  指定された順序: {desired_order}")
            print(f"  適用される順序: {final_columns}")
        
        df_extracted = df_extracted[final_columns]
        
        return df_extracted
        
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません - {file_path}")
        return pd.DataFrame()
    except UnicodeDecodeError as ude:
        print(f"エンコーディングエラー: ファイル '{file_path}' の読み込みに失敗しました。UTF-8 (BOM付き) もしくは cp932 で試しましたが、他のエンコーディングかもしれません。エラー詳細: {ude}")
        return pd.DataFrame()
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        return pd.DataFrame()

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

    # テスト用のファイルパスと日付
    test_tegata_path = '2024年8月度データ/支払手形_202408.csv'
    test_target_date = pd.to_datetime('2024-08-01')

    print(f"--- '{test_tegata_path}' の処理を開始します ---")
    df_tegata = preprocess_shiharai_tegata(test_tegata_path, test_target_date)
    print("------------------------------------------")

    if df_tegata is not None:
        print("\n--- 処理成功: 支払手形データ（全体）---")
        print(df_tegata)
        print("\n--- データ型 ---")
        print(df_tegata.dtypes)
        save_df_to_debug(df_tegata, 'module_shiharaitegata_result')
    else:
        print(f"--- '{test_tegata_path}' の処理に失敗しました ---")
