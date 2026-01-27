import matplotlib.pyplot as plt
import pandas as pd
from config import collection

def analyse_fn():

    analyse_mongo = list(collection.find(({})))
    df = pd.DataFrame(analyse_mongo)
    print("ich analysiere das Dataframe")
    analyse_df= df.drop(['_id','coordinates', 'sensorsId', 'locationsId'], axis='columns')
    print(analyse_df.columns)
    print('Convert to datetime')
    analyse_df['datetime'] = analyse_df['datetime'].apply(lambda x: x['utc'])
    analyse_df['datetime'] = pd.to_datetime(analyse_df['datetime'])
    print("Nun der Pivot der Tabellen ,damit Zeitreihenanalyse möglich wird.")
    df_clean_pivot = analyse_df.pivot_table(index='datetime', columns='target', values='value', aggfunc = 'first')
    print(df_clean_pivot.columns)
    print(df_clean_pivot)

    col_lst = ['co', 'no', 'no2', 'nox', 'o3', 'pm10', 'pm25', 'so2']
    units_dict = {'co': 'ppm', 'no': 'ppm', 'no2': 'ppm', 'nox': 'ppm',
                  'o3': 'ppm', 'pm10': 'µg/m³', 'pm25': 'µg/m³', 'so2': 'ppm'}
    ausreisser_liste = []
    threshold_liste = []

    for col in col_lst:
        mean_col = df_clean_pivot[col].mean()
        threshold = mean_col + (2 * mean_col / 10)  # +20%
        print(f"{col}: Mittelwert={round(mean_col, 4)}, Ausreißer-Grenze={round(threshold, 4)}")

        for datum, wert in df_clean_pivot[col].items():
            if pd.isna(wert):
                continue
            if wert > threshold:
                ausreisser_liste.append({'datum': datum, 'sensor': col, 'wert': round(wert, 4), 'typ': 'Ausreißer'})
            elif wert > mean_col:
                threshold_liste.append({'datum': datum, 'sensor': col, 'wert': round(wert, 4), 'typ': 'Überschreitung'})

    print("\n--- Ausreißer (>20% über Mittelwert) ---")
    for a in ausreisser_liste:
        print(f"{a['datum']} | {a['sensor']}: {a['wert']}")

    print("\n--- Threshold-Überschreitungen (über Mittelwert) ---")
    for t in threshold_liste:
        print(f"{t['datum']} | {t['sensor']}: {t['wert']}")

    fig, axes = plt.subplots(2, 4, figsize=(16, 8))
    axes = axes.flatten()
    for i, col in enumerate(col_lst):
        axes[i].plot(df_clean_pivot.index, df_clean_pivot[col], marker='o')
        axes[i].set_title(f"{col} ({units_dict[col]})")

    plt.tight_layout()
    plt.show()