import pandas as pd
from typing import Optional
import os
from datetime import datetime

def preprocess_haihu(file_path: str, target_date: pd.Timestamp) -> Optional[pd.DataFrame]:
    """
    配賦率CSVを読み込み、縦持ちに変換し、日付と配賦率を追加します。

    処理フロー:
    1. CSVを読み込みます。1行目をカラム名、2行目をデータとして扱います。
    2. 横持ちデータを縦持ちデータに変換します（'部門コード', '業務量割合'）。
    3. 引数で受け取った 'target_date' を '日付' カラムとして追加します。
    4. '業務量割合' の合計値を計算します。
    5. '業務量割合' を合計値で割り、新しい '配賦率' カラムを作成します。

    Args:
        file_path (str): CSVファイルのパス。
        target_date (pd.Timestamp): 処理対象の基準日。

    Returns:
        Optional[pd.DataFrame]: 処理後のDataFrame。エラーの場合はNone。
    """
    try:
        # 1. CSVの読み込み（1行目をヘッダー、2行目をデータとする）
        df = pd.read_csv(file_path, header=0, encoding='utf-8-sig')
        
        if df.empty:
            print("警告: 読み込んだ配賦率データが空です。")
            return None

        # 2. 横持ちから縦持ちへ変換
        df_melted = df.melt(var_name='部門コード', value_name='業務量割合')

        # 3. 日付カラムの追加
        df_melted['日付'] = target_date
        
        # 業務量割合を数値型に変換
        df_melted['業務量割合'] = pd.to_numeric(df_melted['業務量割合'], errors='coerce').fillna(0)

        # 4. 場所別配賦グループの定義
        group_map = {
            'H102200': '本店・東京支店', 'H101100': '本店・東京支店', 'H101210': '本店・東京支店',
            'H101310': '本店・東京支店', 'H104100': '本店・東京支店', 'H102100': '本店・東京支店',
            'H102120': '本店・東京支店',
            'S202100': '札幌支店', 'S201100': '札幌支店', 'S201200': '札幌支店',
            'O502100': '大阪支店', 'O502200': '大阪支店', 'O501100': '大阪支店', 'O501200': '大阪支店',
            'F602100': '福岡支店', 'F602200': '福岡支店', 'F601100': '福岡支店', 'F601200': '福岡支店'
        }

        # 5. group_map に存在する部門コードのみにフィルタリング
        df_melted = df_melted[df_melted['部門コード'].isin(group_map.keys())].copy()

        if df_melted.empty:
            print("警告: group_mapに一致する部門コードが見つかりませんでした。")
            return None

        # 6. フィルタリングされたデータで業務量割合の合計を計算
        total_business_volume = df_melted['業務量割合'].sum()

        if total_business_volume == 0:
            print("警告: 業務量割合の合計が0です。配賦率は計算できません。")
            df_melted['配賦率'] = 0.0
        else:
            # 7. 配賦率カラムの計算と追加
            df_melted['配賦率'] = df_melted['業務量割合'] / total_business_volume
        
        # --- 場所別配賦の処理 ---
        # 8. 場所別配賦グループの割り当て
        df_melted['場所別配賦グループ'] = df_melted['部門コード'].map(group_map)
        
        # 9. 場所別配賦率の計算
        # グループごとの配賦率合計を計算
        group_sum = df_melted.groupby('場所別配賦グループ')['配賦率'].transform('sum')
        
        # ゼロ除算を避ける
        df_melted['場所別配賦率'] = df_melted['配賦率'].divide(group_sum).fillna(0)

        print("情報: 配賦率データの処理が完了しました。")
        return df_melted

    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません - {file_path}")
        return None
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
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

    test_file = '2024年8月度データ/配賦率.csv'
    test_date = pd.to_datetime('2024/08/01')
    
    print(f"--- '{test_file}' の処理をテストします ---")
    df_result = preprocess_haihu(test_file, test_date)
    print("---------------------------------------")

    if df_result is not None:
        print("\n--- 処理成功: 配賦率データ（先頭5行） ---")
        print(df_result.head())
        print(f"\nデータ件数: {len(df_result)}件")
        # デバッグ用にCSV出力
        save_df_to_debug(df_result, 'module_haihu_result')
        
    else:
        print(f"\n--- '{test_file}' の処理に失敗しました ---")
