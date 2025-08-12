import pandas as pd

def aplicar_formatacoes_df(df, options=None):
    if options is None:
        options = {}
    
    df2 = df.copy()

    force_text = set(options.get('force_text', []))
    force_numeric = set(options.get('force_numeric', []))
    force_integer = set(options.get('force_integer', []))
    force_date = set(options.get('force_date', []))

    for col in df2.columns:
        if col in force_text:
            df2[col] = df2[col].astype(object).where(df2[col].notna(), None).apply(lambda x: str(x) if x is not None else x)
        elif col in force_numeric:
            df2[col] = pd.to_numeric(df2[col], errors='coerce')
        elif col in force_integer:
            df2[col] = pd.to_numeric(df2[col], errors='coerce').astype('Int64')
        elif col in force_date:
            df2[col] = pd.to_datetime(df2[col], errors='coerce')
            
    return df2