import pandas as pd
from typing import Optional

def load_bs_classification(file_path: str) -> Optional[pd.DataFrame]:
    """
    部門別BS対象科目.csvを読み込み、分類データをDataFrameとして返します。

    Args:
        file_path (str): CSVファイルのパス。

    Returns:
        Optional[pd.DataFrame]: 勘定科目コード、分類1, 分類2, 分類3を含むDataFrame。エラーの場合はNone。
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        required_columns = ['勘定科目コード', '分類1', '分類2', '分類3']
        if not set(required_columns).issubset(df.columns):
            # cp932で再試行
            try:
                df = pd.read_csv(file_path, encoding='cp932')
                if not set(required_columns).issubset(df.columns):
                    raise ValueError("必須カラムが見つかりません。")
            except Exception:
                missing = set(required_columns) - set(df.columns)
                print(f"エラー: 必須カラム {missing} が '{file_path}' に見つかりません。")
                return None
        
        # 必要なカラムのみを返す
        return df[required_columns]

    except FileNotFoundError:
        print(f"エラー: 分類ファイルが見つかりません - {file_path}")
        return None
    except Exception as e:
        print(f"予期せぬエラーが発生しました ({file_path}読み込み中): {e}")
        return None

if __name__ == '__main__':
    # モジュールのテスト用コード
    csv_file = '部門別BS対象科目.csv'
    
    print(f"--- '{csv_file}' の処理を開始します ---")
    classification_df = load_bs_classification(csv_file)
    print("------------------------------------")

    if classification_df is not None:
        print("\n--- 読み込み成功: 分類データ（先頭5行） ---")
        print(classification_df.head())
        print("\n--- カラム一覧 ---")
        print(classification_df.columns.tolist())
    else:
        print(f"\n'{csv_file}' の処理に失敗しました。") 