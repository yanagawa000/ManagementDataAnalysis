import pandas as pd
from module_suiihyou import preprocess_csv
from module_yoshinzandaka import preprocess_yoshin
from module_motocho import preprocess_motocho
from module_MACROSSzaiko import preprocess_zaiko_step1
from module_shiharaitegata import preprocess_shiharai_tegata
from module_bs_classification import load_bs_classification
from module_location import load_location_data
from module_haihu import preprocess_haihu
from module_haifu_keisan import execute_allocation

def get_user_date() -> pd.Timestamp:
    """
    ユーザーに日付の入力を求め、有効な日付が入力されるまで繰り返す。
    YYYY/MMの形式で入力を受け付け、その月の初日を返す。
    """
    while True:
        date_str = input("基準日を入力してください (例: 2024/08): ")
        try:
            # YYYY/MM 形式をパースし、月の初日として解釈
            user_date = pd.to_datetime(date_str + '/01')
            return user_date
        except ValueError:
            print("エラー: 無効な日付形式です。'YYYY/MM'の形式で入力してください。")

def main():
    """
    各前処理モジュールを実行し、結果を統合・表示するためのメイン関数。
    """
    print("===================================")
    print("          データ前処理開始")
    print("===================================")

    # ユーザーから基準日を取得
    target_date = get_user_date()
    print(f"\n情報: 基準日として '{target_date.strftime('%Y-%m-%d')}' を設定しました。\n")

    all_dataframes = []
    sum_bs1043 = 0.0 # 後続の計算のために初期化

    # --- 1. 推移表データの処理 ---
    # 推移表データ＋商品の合計値を出力
    print("\n--- [1/5] 推移表データの処理を開始します ---")
    suiihyou_path = '2024年8月度データ/推移表_貸借対照表_部門別_2024_上期.csv'
    df_suiihyou, sum_bs1043_series = preprocess_csv(suiihyou_path)
    if df_suiihyou is not None:
        # ユーザー指定月に対応する合計金額を取得
        sum_bs1043 = sum_bs1043_series.get(target_date, 0.0)
        print(f"\n[結果] BS1043の合計金額 ({target_date.strftime('%Y-%m')}): {sum_bs1043:,.0f}")

        # df_suiihyouをtarget_dateでフィルタリング
        df_suiihyou['日付'] = pd.to_datetime(df_suiihyou['日付'])
        df_suiihyou = df_suiihyou[df_suiihyou['日付'] == target_date].copy()
        
        print("\n[結果] 推移表データ (先頭5行):")
        print(df_suiihyou.head())
        
        all_dataframes.append(df_suiihyou)
    else:
        print("\n[結果] 推移表データの処理に失敗しました。")
    print("--- [1/5] 推移表データの処理を完了しました ---\n")

    # --- 2. 与信残高表データの処理 ---
    # 受取手形金額の部門別を出力、メタ情報から日付を取得
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
    # 109801と491701のデータを出力(商品評価損と輸入消費税)
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

    # --- 特販部在庫の計算 ---
    if sum_bs1043 is not None and (df_motocho_2 is not None and not df_motocho_2.empty) and (df_zaiko is not None and not df_zaiko.empty):
        print("--- [追加処理] 特販部在庫の計算を開始します ---")
        try:
            # df_motocho_2は1行しかないので、.iloc[0]で値を取得
            motocho_value = df_motocho_2['金額'].iloc[0]
            
            # df_zaikoの金額を合計
            zaiko_total_value = df_zaiko['金額'].sum()
            
            # 特販部在庫を計算
            tokuhanzaiko = sum_bs1043 - motocho_value - zaiko_total_value
            
            print(f"[計算内訳] BS1043合計: {sum_bs1043:,.0f}")
            print(f"[計算内訳] 元帳(491701)金額: {motocho_value:,.0f}")
            print(f"[計算内訳] 在庫合計金額: {zaiko_total_value:,.0f}")
            print(f"[結果] 計算された特販部在庫: {tokuhanzaiko:,.0f}")

            # 新しいDataFrameを作成
            df_tokuhanzaiko = pd.DataFrame({
                '日付': [zaiko_date],
                '部門コード': ['H102200'],
                '部門名': ['本店特販部'],
                '勘定科目コード': ['BS1043'],
                '勘定科目名': ['★商品'],
                '金額': [tokuhanzaiko]
            })
            
            all_dataframes.append(df_tokuhanzaiko)
            print("情報: 特販部在庫のデータをDataFrameとして追加しました。")

        except Exception as e:
            print(f"エラー: 特販部在庫の計算中にエラーが発生しました: {e}")
        print("--- [追加処理] 特販部在庫の計算を完了しました ---\n")

    # --- 5. 支払手形データの処理 ---
    print("--- [5/5] 支払手形データの処理を開始します ---")
    tegata_path = '2024年8月度データ/支払手形_202408.csv'
    df_tegata = preprocess_shiharai_tegata(tegata_path, target_date)
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

        # --- 分類情報の読み込みとマージ ---
        print("\n--- 分類情報をマージします ---")
        classification_file = '部門別BS対象科目.csv'
        df_classification = load_bs_classification(classification_file)
        
        if df_classification is not None:
            # merged_dfの「勘定科目コード」を文字列型に変換してからマージ
            merged_df['勘定科目コード'] = merged_df['勘定科目コード'].astype(str)
            df_classification['勘定科目コード'] = df_classification['勘定科目コード'].astype(str)
            
            merged_df = pd.merge(
                merged_df, 
                df_classification, 
                on='勘定科目コード', 
                how='left'
            )
            print("情報: 分類情報（分類1, 分類2, 分類3）をマージしました。")
        else:
            print("警告: 分類情報のマージに失敗したため、分類列は追加されませんでした。")
        
        # --- 場所情報の読み込みと事前マージ（共通レコード分離のため） ---
        print("\n--- 場所情報をマージします ---")
        location_file = '部門コード_場所.csv'
        df_location = load_location_data(location_file)

        if df_location is not None:
            df_location['部門コード'] = df_location['部門コード'].astype(str)
            merged_df['部門コード'] = merged_df['部門コード'].astype(str)
            # 共通レコード分離に必要な「場所」列のみを一時的にマージする
            merged_df = pd.merge(merged_df, df_location[['部門コード', '場所']], on='部門コード', how='left')
            print("情報: 共通レコード分離のために場所情報を一時的にマージしました。")
        else:
            print("警告: 場所情報ファイルが読み込めなかったため、配賦計算は正しく行われない可能性があります。")

        # 「場所」が '全店共通' または '場所共通' のレコードを分離
        # この処理のために、事前に場所情報をマージしておく必要がある
        is_zen_kyoutuu = merged_df['場所'] == '全店共通'
        is_basho_kyoutuu = merged_df['場所'] == '場所共通'
        
        df_zen_kyoutuu = merged_df[is_zen_kyoutuu].copy()
        df_basho_kyoutuu = merged_df[is_basho_kyoutuu].copy()
        
        # 元のDataFrameからは配賦対象の共通レコードと、重複を避けるため場所・部門名を削除
        columns_to_drop = ['場所', '部門名']
        df_main_data = merged_df.drop(columns=columns_to_drop)[~is_zen_kyoutuu & ~is_basho_kyoutuu].copy()

        # 最終的にマージするデータフレームを格納するリスト
        final_data_parts = [df_main_data]

        print("\n--- 共通レコードの分離 ---")
        print(f"情報: '全店共通' のレコードを {len(df_zen_kyoutuu)} 件分離しました。")
        print(f"情報: '場所共通' のレコードを {len(df_basho_kyoutuu)} 件分離しました。")
        print(f"情報: 分離後のメインデータは {len(df_main_data)} 件になりました。")

        # --- 配賦率データの準備 ---
        print("\n--- 配賦率データを読み込みます ---")
        haihu_path = '2024年8月度データ/配賦率.csv'
        df_haihu = preprocess_haihu(haihu_path, target_date)
        
        # --- 配賦計算の実行 ---
        if df_haihu is not None and not df_haihu.empty:
            print("\n--- 配賦計算モジュールを実行します ---")
            df_allocated = execute_allocation(df_zen_kyoutuu, df_basho_kyoutuu, df_haihu, target_date)
            if not df_allocated.empty:
                final_data_parts.append(df_allocated)
                print("--- 配賦計算モジュールの実行が完了しました ---")
            else:
                print("情報: 配賦計算の結果、追加されるデータはありませんでした。")
        else:
            print("警告: 配賦率データの取得に失敗したか、データが空のため、配賦計算はスキップされました。")

        # --- 全てのデータパーツを最終的に統合 ---
        print("\n--- 全てのデータパーツを統合します ---")
        print(f"情報: {len(final_data_parts)}個のデータパーツ（本体、配賦結果）をマージします。")
        merged_df = pd.concat(final_data_parts, ignore_index=True)
        print("情報: 最終的なデータ統合が完了しました。")

        # --- 最後に部門名と場所の情報を再付与 ---
        if df_location is not None:
            print("\n--- 部門名と場所の情報を最終データに付与します ---")
            # 既にdf_locationで型変換済みなので、merged_df側のみ実施
            merged_df['部門コード'] = merged_df['部門コード'].astype(str)
            merged_df = pd.merge(merged_df, df_location, on='部門コード', how='left')
            print("情報: 部門名と場所の付与が完了しました。")
        else:
            print("警告: 場所情報データが読み込めなかったため、部門名と場所は付与されませんでした。")

        # 見やすさのために日付と部門コードでソート
        merged_df.sort_values(by=['日付', '部門コード'], na_position='last', inplace=True)
        
        output_path = 'combined_data.csv'
        try:
            merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"成功: マージしたデータを '{output_path}' に出力しました。")
        except Exception as e:
            print(f"エラー: CSVファイルへの出力中にエラーが発生しました: {e}")
        
        # --- デバッグ用CSVを処理の最後に出力 ---
        print("\n--- デバッグ用ファイルを出力します ---")
        try:
            output_path_zen = 'debug_df_zen_kyoutuu.csv'
            df_zen_kyoutuu.to_csv(output_path_zen, index=False, encoding='utf-8-sig')
            print(f"情報: デバッグ用ファイル '{output_path_zen}' を出力しました。")
        except Exception as e:
            print(f"エラー: zen_kyoutuu のデバッグCSV出力中にエラー: {e}")
        
        try:
            output_path_basho = 'debug_df_basho_kyoutuu.csv'
            df_basho_kyoutuu.to_csv(output_path_basho, index=False, encoding='utf-8-sig')
            print(f"情報: デバッグ用ファイル '{output_path_basho}' を出力しました。")
        except Exception as e:
            print(f"エラー: basho_kyoutuu のデバッグCSV出力中にエラー: {e}")

    print("===================================")
    print("        全てのデータ処理が完了")
    print("===================================")

if __name__ == '__main__':
    main()
