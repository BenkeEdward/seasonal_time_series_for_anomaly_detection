import zipfile
import pandas as pd

all_dfs = []
with zipfile.ZipFile('../data/raw_zip/dataset.zip', 'r') as z:
    for file_info in z.infolist():
        if file_info.filename.endswith('.csv'):
            with z.open(file_info) as f:
                df = pd.read_csv(
                    f,
                    parse_dates=['timestamp'],
                    
                    usecols=['timestamp', 'value'], 
                    na_values=['NA', 'null', 'NaN', '']  
                )
                all_dfs.append(df)


combined_df = pd.concat(all_dfs, ignore_index=True)
combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
combined_df.to_csv('../data/processed/cleaned_dataset.csv')
