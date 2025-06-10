import pandas as pd
from typing import List, Optional

def execute_allocation(
    df_zen_kyoutuu: pd.DataFrame, 
    df_basho_kyoutuu: pd.DataFrame, 
    df_haihu: pd.DataFrame, 
    target_date: pd.Timestamp
) -> pd.DataFrame:
    """
    全店共通および場所共通データの配賦計算を実行し、結果を一つのDataFrameに統合して返す。

    Args:
        df_zen_kyoutuu (pd.DataFrame): 場所が「全店共通」のデータ。
        df_basho_kyoutuu (pd.DataFrame): 場所が「場所共通」のデータ。
        df_haihu (pd.DataFrame): 配賦率データ。
        target_date (pd.Timestamp): 処理対象の基準日。

    Returns:
        pd.DataFrame: 全ての配賦計算結果を統合したデータフレーム。
    """
    allocation_results: List[pd.DataFrame] = []

    # --- 全店共通データ（運転資本）の配賦計算 ---
    print("\n--- 全店共通データ（運転資本）の配賦計算を開始します ---")
    if not df_zen_kyoutuu.empty and df_haihu is not None and not df_haihu.empty:
        try:
            sum_shisan = df_zen_kyoutuu[df_zen_kyoutuu['分類2'] == '運転資本(資産)']['金額'].sum()
            sum_fusai = df_zen_kyoutuu[df_zen_kyoutuu['分類2'] == '運転資本(負債)']['金額'].sum()
            untenshihon_total = sum_shisan - sum_fusai
            print(f"情報: 配賦対象の運転資本合計 (資産 - 負債): {untenshihon_total:,.0f}")

            if untenshihon_total != 0:
                df_untenshihon_anbun = pd.DataFrame({
                    '日付': target_date, 
                    '部門コード': df_haihu['部門コード'].astype(str),
                    '金額': untenshihon_total * df_haihu['配賦率'],
                    '分類1': '運転資本', '分類2': '運転資本_全店共通', '分類3': '', '勘定科目コード': '', '勘定科目名': ''
                })
                allocation_results.append(df_untenshihon_anbun)
                print("[結果] 全店共通（運転資本）の配賦結果を作成しました。")

        except Exception as e:
            print(f"エラー: 全店共通（運転資本）の配賦計算中にエラーが発生しました: {e}")
    else:
        print("情報: '全店共通'データがないか配賦率データが読み込めないため、運転資本の配賦計算はスキップされました。")
    print("--- 全店共通データ（運転資本）の配賦計算を完了しました ---")

    # --- 全店共通データ（固定資産）の配賦計算 ---
    print("\n--- 全店共通データ（固定資産）の配賦計算を開始します ---")
    if not df_zen_kyoutuu.empty and df_haihu is not None and not df_haihu.empty:
        try:
            sum_shisan_kotei = df_zen_kyoutuu[df_zen_kyoutuu['分類2'] == '固定資産(資産)']['金額'].sum()
            sum_fusai_kotei = df_zen_kyoutuu[df_zen_kyoutuu['分類2'] == '固定資産(負債)']['金額'].sum()
            koteishisan_total = sum_shisan_kotei - sum_fusai_kotei
            print(f"情報: 配賦対象の固定資産合計 (資産 - 負債): {koteishisan_total:,.0f}")

            if koteishisan_total != 0:
                df_koteishisan_anbun = pd.DataFrame({
                    '日付': target_date, 
                    '部門コード': df_haihu['部門コード'].astype(str),
                    '金額': koteishisan_total * df_haihu['配賦率'],
                    '分類1': '固定資産', '分類2': '固定資産_全店共通', '分類3': '', '勘定科目コード': '', '勘定科目名': ''
                })
                allocation_results.append(df_koteishisan_anbun)
                print("[結果] 全店共通（固定資産）の配賦結果を作成しました。")

        except Exception as e:
            print(f"エラー: 全店共通（固定資産）の配賦計算中にエラーが発生しました: {e}")
    else:
        print("情報: '全店共通'データがないか配賦率データが読み込めないため、固定資産の配賦計算はスキップされました。")
    print("--- 全店共通データ（固定資産）の配賦計算を完了しました ---")

    # --- 場所共通データの配賦計算 ---
    print("\n--- 場所共通データの配賦計算を開始します ---")
    target_departments_basho = ['H100100', 'S200100', 'O500100', 'F600100']
    all_basho_haifu_dfs = []

    if not df_basho_kyoutuu.empty and df_haihu is not None and not df_haihu.empty:
        for dept_code in target_departments_basho:
            print(f"\n--- 場所共通データ ({dept_code}) の配賦処理を開始 ---")
            df_dept_basho = df_basho_kyoutuu[df_basho_kyoutuu['部門コード'] == dept_code]
            if df_dept_basho.empty:
                print(f"情報: 場所共通データに部門コード '{dept_code}' が見つからないためスキップします。")
                continue

            sum_shisan_unten = df_dept_basho[df_dept_basho['分類2'] == '運転資本(資産)']['金額'].sum()
            sum_fusai_unten = df_dept_basho[df_dept_basho['分類2'] == '運転資本(負債)']['金額'].sum()
            untenshihon_total = sum_shisan_unten - sum_fusai_unten
            print(f"情報: 配賦対象の運転資本合計 ({dept_code}): {untenshihon_total:,.0f}")

            sum_shisan_kotei = df_dept_basho[df_dept_basho['分類2'] == '固定資産(資産)']['金額'].sum()
            sum_fusai_kotei = df_dept_basho[df_dept_basho['分類2'] == '固定資産(負債)']['金額'].sum()
            koteishisan_total = sum_shisan_kotei - sum_fusai_kotei
            print(f"情報: 配賦対象の固定資産合計 ({dept_code}): {koteishisan_total:,.0f}")

            prefix = dept_code[:3]
            df_haihu_target = df_haihu[df_haihu['部門コード'].str.startswith(prefix)].copy()
            if df_haihu_target.empty:
                print(f"警告: 部門プレフィックス '{prefix}' に一致する配賦先が配賦率データにないためスキップします。")
                continue
            
            if untenshihon_total != 0:
                df_unten_anbun = pd.DataFrame({
                    '日付': target_date, 
                    '部門コード': df_haihu_target['部門コード'].astype(str),
                    '金額': untenshihon_total * df_haihu_target['場所別配賦率'],
                    '分類1': '運転資本', '分類2': '運転資本_場所共通', '分類3': '', '勘定科目コード': '', '勘定科目名': ''
                })
                all_basho_haifu_dfs.append(df_unten_anbun)
                print("[結果] 場所共通（運転資本）の配賦結果を作成しました。")

            if koteishisan_total != 0:
                df_kotei_anbun = pd.DataFrame({
                    '日付': target_date, 
                    '部門コード': df_haihu_target['部門コード'].astype(str),
                    '金額': koteishisan_total * df_haihu_target['場所別配賦率'],
                    '分類1': '固定資産', '分類2': '固定資産_場所共通', '分類3': '', '勘定科目コード': '', '勘定科目名': ''
                })
                all_basho_haifu_dfs.append(df_kotei_anbun)
                print("[結果] 場所共通（固定資産）の配賦結果を作成しました。")

        if all_basho_haifu_dfs:
            df_basho_haifu_kekka = pd.concat(all_basho_haifu_dfs, ignore_index=True)
            allocation_results.append(df_basho_haifu_kekka)
            print("\n情報: 場所共通データの配賦計算結果を統合しました。")
    else:
        print("情報: '場所共通'データがないか配賦率データが読み込めないため、配賦計算はスキップされました。")
    print("--- 場所共通データの配賦計算を完了しました ---")

    if not allocation_results:
        return pd.DataFrame()

    return pd.concat(allocation_results, ignore_index=True)

if __name__ == '__main__':
    # モジュールのテスト用コード
    import os
    from datetime import datetime
    from module_location import load_location_data
    from module_haihu import preprocess_haihu

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

    print("--- 配賦計算モジュールのテストを開始します ---")

    # --- テスト用データの準備 ---
    target_date = pd.to_datetime('2024-08-01')
    
    # 場所データ
    location_file = '部門コード_場所.csv'
    df_location = load_location_data(location_file)
    if df_location is None:
        raise ValueError("テスト失敗: 場所データの読み込みに失敗しました。")

    # 配賦率データ
    haihu_path = '2024年8月度データ/配賦率.csv'
    df_haihu = preprocess_haihu(haihu_path, target_date)
    if df_haihu is None:
        raise ValueError("テスト失敗: 配賦率データの読み込みに失敗しました。")

    # 全店共通・場所共通のダミーデータ作成
    # 実際のデータ構造に合わせて作成
    common_data = {
        '日付': [target_date, target_date, target_date, target_date, target_date, target_date, target_date, target_date],
        '部門コード': ['A900000', 'A900000', 'A100200', 'A100200', 'H100100', 'H100100', 'S200100', 'S200100'],
        '分類1': ['運転資本', '運転資本', '固定資産', '固定資産', '運転資本', '運転資本', '固定資産', '固定資産'],
        '分類2': ['運転資本(資産)', '運転資本(負債)', '固定資産(資産)', '固定資産(負債)', '運転資本(資産)', '運転資本(負債)', '固定資産(資産)', '固定資産(負債)'],
        '場所': ['全店共通', '全店共通', '全店共通', '全店共通', '場所共通', '場所共通', '場所共通', '場所共通'],
        '金額': [1000, 200, 5000, 1000, 500, 100, 2000, 400]
    }
    df_common_test = pd.DataFrame(common_data)
    df_zen_kyoutuu_test = df_common_test[df_common_test['場所'] == '全店共通']
    df_basho_kyoutuu_test = df_common_test[df_common_test['場所'] == '場所共通']
    
    print("\n[テストデータ] 全店共通:")
    print(df_zen_kyoutuu_test)
    print("\n[テストデータ] 場所共通:")
    print(df_basho_kyoutuu_test)
    print("\n[テストデータ] 配賦率:")
    print(df_haihu.head())
    
    # --- 関数実行 ---
    print("\n--- execute_allocation関数を実行します ---")
    df_result = execute_allocation(df_zen_kyoutuu_test, df_basho_kyoutuu_test, df_haihu, target_date)
    print("------------------------------------------")

    # --- 結果確認 ---
    if df_result is not None and not df_result.empty:
        print("\n--- 処理成功: 配賦計算結果（全体）---")
        print(df_result)
        save_df_to_debug(df_result, 'module_haifu_keisan_result')
    else:
        print("\n--- 処理失敗、または結果が空でした ---")

    print("\n--- 配賦計算モジュールのテストを完了しました ---") 