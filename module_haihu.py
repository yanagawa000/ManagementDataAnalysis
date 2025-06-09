import pandas as pd
from typing import Optional

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

        # 4. 業務量割合の合計を計算
        total_business_volume = df_melted['業務量割合'].sum()
        
        if total_business_volume == 0:
            print("警告: 業務量割合の合計が0です。配賦率は計算できません。")
            df_melted['配賦率'] = 0.0
        else:
            # 5. 配賦率カラムの計算と追加
            df_melted['配賦率'] = df_melted['業務量割合'] / total_business_volume
        
        # --- 場所別配賦の処理 ---
        # 1. 場所別配賦グループの定義
        group_map = {
            'H102200': '本店・東京支店', 'H101100': '本店・東京支店', 'H101210': '本店・東京支店',
            'H101310': '本店・東京支店', 'H104100': '本店・東京支店', 'H102100': '本店・東京支店',
            'H102120': '本店・東京支店',
            'S202100': '札幌支店', 'S201100': '札幌支店', 'S201200': '札幌支店',
            'O502100': '大阪支店', 'O502200': '大阪支店', 'O501100': '大阪支店', 'O501200': '大阪支店',
            'F602100': '福岡支店', 'F602200': '福岡支店', 'F601100': '福岡支店', 'F601200': '福岡支店'
        }
        
        # 2. グループの割り当てとフィルタリング
        df_melted['場所別配賦グループ'] = df_melted['部門コード'].map(group_map).fillna('その他')
        df_melted = df_melted[df_melted['場所別配賦グループ'] != 'その他'].copy()

        # 3. 場所別配賦率の計算
        # グループごとの配賦率合計を計算
        group_sum = df_melted.groupby('場所別配賦グループ')['配賦率'].transform('sum')
        
        # ゼロ除算を避ける
        # group_sumが0の場所は、場所別配賦率も0とする
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
    csv_file = '2024年8月度データ/配賦率.csv'
    target_date_for_test = pd.Timestamp('2024-08-01')
    output_csv_file = 'processed_haihu_output.csv' # 出力ファイル名
    
    print(f"--- '{csv_file}' の処理を開始します ---")
    processed_df = preprocess_haihu(csv_file, target_date_for_test)
    print("------------------------------------")

    if processed_df is not None:
        print("\n--- 処理後のDataFrame（全体） ---")
        pd.set_option('display.max_rows', None) # 全ての行を表示
        print(processed_df)
        pd.reset_option('display.max_rows')
        
        print("\n--- 処理後のDataFrameのカラム一覧 ---")
        print(processed_df.columns.tolist())
        
        print(f"\n業務量割合の合計: {processed_df['業務量割合'].sum()}")
        print(f"配賦率の合計: {processed_df['配賦率'].sum()}")

        try:
            processed_df.to_csv(output_csv_file, index=False, encoding='utf-8-sig')
            print(f"\n成功: 処理結果を '{output_csv_file}' に出力しました。")
        except Exception as e:
            print(f"\nエラー: CSVファイル '{output_csv_file}' の出力中にエラーが発生しました: {e}")
    else:
        print(f"\n'{csv_file}' の処理に失敗しました。")
