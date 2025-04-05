import pandas as pd
import os

combined_matches_df = pd.read_csv('input_files\combined data\matches.csv')
combined_matches_df['date'] = pd.to_datetime(combined_matches_df['date'], format='%d-%m-%Y')
combined_deliveries_df = pd.read_csv('input_files\combined data\deliveries.csv')
for i in combined_matches_df.id:
    df_mat = combined_matches_df[combined_matches_df['id'] == i] 
    df_del = combined_deliveries_df[combined_deliveries_df['match_id'] == i]
    os.makedirs(f'input_files\saperated data\match_{i}', exist_ok=True)
    df_mat.to_parquet(f'input_files\saperated data\match_{i}\match_{i}.parquet', index=False)
    df_del.to_parquet(f'input_files\saperated data\match_{i}\deliveries_{i}.parquet', index=False)
print('Process completed')