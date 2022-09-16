import pandas as pd


def repair_columnTags(df):
    pattern = '|'.join(
        ['[\}]', '[\{]', '[\s]', '[\:]', 'count', '[0-9]', 'label', "'", '[\/]'])
    df['tags'] = df['quantity_looked_by_tag'].astype(
        str).str.replace(pattern, "", regex=True)
    df['tags'] = df['tags'].str.replace('^\,', "", regex=True)
    df['tags'] = df['tags'].str.split(',')
    df = df.explode('tags')
    return df


def repair_columnQuantityTags(df):
    pattern = '|'.join(["'", ":"])
    df['quantity_looked_by_tag'] = df['quantity_looked_by_tag'].astype(str).str.replace(
        pattern, "", regex=True)
    df['quantity_looked_by_tag'] = df['quantity_looked_by_tag'].str.extract(
        "([0-9])")
    return df


def repair_clientName(df):
    df['client_name'] = df['client_name'].astype(
        str).str.replace("'", "", regex=True)
    return df


def repair_data(df):
    df['computed_date'] = pd.to_datetime(df['computed_date'])
    df['computed_date'] = df['computed_date'].dt.strftime('%d/%m/%Y')
    df['createdAt'] = pd.to_datetime(df['createdAt'])
    df['createdAt'] = df['createdAt'].dt.strftime('%d/%m/%Y')
    return df
