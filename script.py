import pyspark.pandas as ps
import sys
from pathlib import Path
from pyspark import SparkContext, SparkConf


def func(path_to_data, path_to_result):
    
    path_crime = str(Path(path_to_data, "crime.csv"))
    path_offense_codes = str(Path(path_to_data, "offense_codes.csv"))

    df = ps.read_csv(path_crime)
    df1 = ps.read_csv(path_offense_codes)

    # Удалим дубликаты из таблицы с расшифровкой кодов
    df1 = df1.drop_duplicates()

    df1 = df1.drop_duplicates("CODE", keep="first")

    # Удалим дубликаты из основной таблицы

    df = df.drop_duplicates()
    df["year_month"] = df["YEAR"].astype(str) + "_" + df["MONTH"].astype(str)

    # Посчитаем общее число преступлений, средние значения долготы и широты
    res = df.loc[:,["DISTRICT", "INCIDENT_NUMBER", "Lat", "Long"]].groupby("DISTRICT").agg({"INCIDENT_NUMBER":"count", "Lat": "mean", "Long": "mean"})
    temp = df.groupby(["DISTRICT", "year_month"]).agg("count")
    temp = temp.reset_index()
    temp_res = temp.groupby("DISTRICT").median()["INCIDENT_NUMBER"]
    temp_res = ps.DataFrame(temp_res)
    temp_res = temp_res.rename(columns={"INCIDENT_NUMBER": "median_crimes_month"})
    res1 = res.join(temp_res)

    # Найдем наиболее частые типа преступлений по районам
    temp_1 = df.groupby(["DISTRICT", "OFFENSE_CODE"]).agg({"INCIDENT_NUMBER":"count"}).sort_index()
    temp_2 = temp_1.reset_index(level=[0, 1])
    temp_3 = temp_2.groupby("DISTRICT").apply(lambda x: x.sort_values(by="INCIDENT_NUMBER", ascending=False).head(3))
    temp_3 = temp_3.set_index("DISTRICT")
    temp_3 = temp_3.reset_index()
    temp_4 = temp_3.merge(df1, left_on='OFFENSE_CODE', right_on='CODE', how="left")
    temp_4["NAME"] = temp_4["NAME"].apply(lambda x: x if '-' not in x else x.split('-')[0])
    temp_5 = temp_4.groupby("DISTRICT")["NAME"].apply(lambda x: ', '.join(x))
    temp_6 = ps.DataFrame(temp_5)

    last_res = res1.join(temp_6, how="left")
    last_res = last_res.rename(columns={"INCIDENT_NUMBER": "crimes_total", "median_crimes_month": "crimes_monthly", "NAME": "frequent_crime_types", "Lat": "lat", "Long": "lng"})

    columns = ["crimes_total", "crimes_monthly", "frequent_crime_types", "lat", "lng"]

    last_res = last_res.reindex(columns=columns)
    last_res.to_parquet(path_to_result)
    # last_res.to_csv(path_to_result)


if __name__ == '__main__':
    conf = SparkConf().setAppName("app")
    sc = SparkContext(conf=conf)

    path_to_data = str(sys.argv[1])
    path_to_result = str(sys.argv[2])

    # print(f"path_to_data {path_to_data}, path_to_result {path_to_result}")

    func(path_to_data, path_to_result)