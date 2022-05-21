from pathlib import Path

import pandas as pd

if __name__ == '__main__':
    PATH = "./human_activity_raw_sensor_data"
    SENSOR_ID = 5887
    SAMPLING_RATE = "5min"

    df_concat = {"value_id" : [],
                 "sensor_id":[],
                 "timestamp":[],
                 "value":[] }

    df_concat = pd.DataFrame(df_concat)

    df_concat["value"] = pd.to_numeric( df_concat.value, errors='coerce').fillna(0).astype(int)
    df_concat["sensor_id"] = pd.to_numeric(df_concat.sensor_id, errors='coerce').fillna(0).astype(int)

    filelist = Path(PATH).glob('sensor_int_part_*')
    for infile in sorted(filelist):

        print(infile)
        df = pd.read_csv(infile, names=["value_id","sensor_id","timestamp","value"], header=0)

        df['timestamp'] = df['timestamp'].astype('datetime64[s]')
        df["timestamp"] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')

        df["value"] = pd.to_numeric(df.value, errors='coerce').fillna(0).astype(int)
        df["sensor_id"] = pd.to_numeric(df.sensor_id, errors='coerce').fillna(0).astype(int)

        df = df[(df["sensor_id"] == SENSOR_ID)]

        df = df.groupby(pd.to_datetime(df.timestamp).dt.ceil(SAMPLING_RATE), as_index=False).last()

        df = df.set_index('timestamp')
        df = df.reset_index()
        df.asfreq(SAMPLING_RATE)
        df_concat = pd.concat([df_concat, df])

    df_concat.to_csv(PATH  + "5887.csv", index=False)
