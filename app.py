import pandas as pd
from module_suiihyou import preprocess_csv
from module_yoshinzandaka import preprocess_yoshin
from module_motocho import preprocess_motocho
from module_MACROSSzaiko import preprocess_zaiko_step1
from module_shiharaitegata import preprocess_shiharai_tegata

def main():
    """
    各前処理モジュールを実行し、結果を統合・表示するためのメイン関数。
    """
    print("===================================")
    print("          データ前処理開始")
    print("===================================")

    all_dataframes = []

    # --- 1. 推移表データの処理 ---
    print("\n--- [1/5] 推移表データの処理を開始します ---")
    suiihyou_path = '2024年8月度データ/推移表_貸借対照表_部門別_2024_上期.csv'
    df_suiihyou, sum_bs1043 = preprocess_csv(suiihyou_path)
    if df_suiihyou is not None:
        print("\n[結果] 推移表データ (先頭5行):")
        print(df_suiihyou.head())
        print(f"\n[結果] BS1043の合計金額: {sum_bs1043:,.0f}")
        all_dataframes.append(df_suiihyou)
    else:
        print("\n[結果] 推移表データの処理に失敗しました。")
    print("--- [1/5] 推移表データの処理を完了しました ---\n")

    # --- 2. 与信残高表データの処理 ---
    print("--- [2/5] 与信残高表データの処理を開始します ---")
    yoshin_path = '2024年8月度データ/与信残高表_202408.csv'
    df_yoshin, yoshin_date = preprocess_yoshin(yoshin_path)
    if df_yoshin is not None:
        print("\n[結果] 与信残高表データ (先頭5行):")
        print(df_yoshin.head())
        print(f"\n[結果] 取得した日付メタ情報: {yoshin_date}")
        all_dataframes.append(df_yoshin)
    else:
        print("\n[結果] 与信残高表データの処理に失敗しました。")
    print("--- [2/5] 与信残高表データの処理を完了しました ---\n")

    # --- 3. MAC元帳データの処理 ---
    print("--- [3/5] MAC元帳データの処理を開始します ---")
    motocho_path = '2024年8月度データ/MAC元帳202408.csv'
    df_motocho_1, df_motocho_2 = preprocess_motocho(motocho_path)
    if df_motocho_1 is not None and not df_motocho_1.empty:
        print("\n[結果] MAC元帳データ (109801):")
        print(df_motocho_1)
        all_dataframes.append(df_motocho_1)
    if df_motocho_2 is not None and not df_motocho_2.empty:
        print("\n[結果] MAC元帳データ (491701):")
        print(df_motocho_2)
        all_dataframes.append(df_motocho_2)
    if df_motocho_1 is None or df_motocho_2 is None:
        print("\n[結果] MAC元帳データの処理に失敗しました。")
    print("--- [3/5] MAC元帳データの処理を完了しました ---\n")

    # --- 4. MACROSS在庫データの処理 ---
    print("--- [4/5] MACROSS在庫データの処理を開始します ---")
    zaiko_path = '2024年8月度データ/在庫一覧_202408.csv'
    df_zaiko, zaiko_date = preprocess_zaiko_step1(zaiko_path)
    if df_zaiko is not None:
        print("\n[結果] MACROSS在庫データ (先頭5行):")
        print(df_zaiko.head())
        print(f"\n[結果] 取得した基準日の月初: {zaiko_date.strftime('%Y-%m-%d') if zaiko_date else 'N/A'}")
        all_dataframes.append(df_zaiko)
    else:
        print("\n[結果] MACROSS在庫データの処理に失敗しました。")
    print("--- [4/5] MACROSS在庫データの処理を完了しました ---\n")

    # --- 5. 支払手形データの処理 ---
    print("--- [5/5] 支払手形データの処理を開始します ---")
    tegata_path = '2024年8月度データ/支払手形_202408.csv'
    df_tegata = preprocess_shiharai_tegata(tegata_path)
    if df_tegata is not None:
        print("\n[結果] 支払手形データ (先頭5行):")
        print(df_tegata.head())
        all_dataframes.append(df_tegata)
    else:
        print("\n[結果] 支払手形データの処理に失敗しました。")
    print("--- [5/5] 支払手形データの処理を完了しました ---\n")

    # --- 全てのDataFrameをマージしてCSVに出力 ---
    if not all_dataframes:
        print("エラー: マージできるDataFrameがありません。")
    else:
        print("\n--- 全てのDataFrameをマージします ---")
        merged_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"情報: {len(all_dataframes)}個のDataFrameをマージしました。総行数: {len(merged_df)}")

        # 見やすさのために日付と部門コードでソート
        merged_df.sort_values(by=['日付', '部門コード'], na_position='last', inplace=True)
        
        output_path = 'combined_data.csv'
        try:
            merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"成功: マージしたデータを '{output_path}' に出力しました。")
        except Exception as e:
            print(f"エラー: CSVファイルへの出力中にエラーが発生しました: {e}")

    print("===================================")
    print("        全てのデータ処理が完了")
    print("===================================")

if __name__ == '__main__':
    main()
