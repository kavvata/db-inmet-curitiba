from csv import DictReader
from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True)
class Cols:
    DATA = "DATA (YYYY-MM-DD)"
    HORA = "HORA (UTC)"
    DT = "DATA-HORA"
    PRECIPITACAO_TOTAL_MM = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"
    PRESSAO_NIVEL_ESTACAO_MB = "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"
    PRESSAO_MAX_ANT_MB = "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)"
    PRESSAO_MIN_ANT_MB = "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)"
    RADIACAO_GLOBAL_KJ_M2 = "RADIACAO GLOBAL (KJ/m²)"
    TEMP_BULBO_SECO_C = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"
    TEMP_ORVALHO_C = "TEMPERATURA DO PONTO DE ORVALHO (°C)"
    TEMP_MAX_ANT_C = "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)"
    TEMP_MIN_ANT_C = "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)"
    TEMP_ORVALHO_MAX_ANT_C = "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)"
    TEMP_ORVALHO_MIN_ANT_C = "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)"
    UMIDADE_REL_MAX_ANT = "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)"
    UMIDADE_REL_MIN_ANT = "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)"
    UMIDADE_REL_HORARIA = "UMIDADE RELATIVA DO AR, HORARIA (%)"
    VENTO_DIRECAO_GRAUS = "VENTO, DIREÇÃO HORARIA (gr) (° (gr))"
    VENTO_RAJADA_MAX_MS = "VENTO, RAJADA MAXIMA (m/s)"
    VENTO_VELOCIDADE_MS = "VENTO, VELOCIDADE HORARIA (m/s)"


def split_into_frames(df: pd.DataFrame) -> list[pd.DataFrame]:
    col_df_list: list[pd.DataFrame] = []
    for col in df.columns[:-1]:
        it = range(2003, 2024 + 1)
        col_df = pd.DataFrame(columns=[it])

        # TODO: parse datetime
        for ano in it:
            some: pd.DataFrame | pd.Series = df[col].loc[
                (df["DATA-HORA"] >= pd.to_datetime(f"{ano}-01-01", utc=True))
                & (df["DATA-HORA"] <= pd.to_datetime(f"{ano}-12-31", utc=True))
            ]

            col_df[ano] = some

        print(col_df.head())
        col_df_list.append(col_df)

    return col_df_list


def main():
    with open(
        "./inmet_dados_historicos_curitiba/01.normalizer.header/some.csv",
    ) as fp:
        csv_reader = DictReader(fp, delimiter=";")

        df = pd.DataFrame(csv_reader)
        df["DATA-HORA"] = pd.to_datetime(
            df[Cols.DATA] + " " + df[Cols.HORA],
            format="mixed",
            utc=True,
        )

        df.sort_values(
            by="DATA-HORA",
            inplace=True,
        )

        df.drop(
            [Cols.DATA, Cols.HORA, ""],
            inplace=True,
            axis=1,
        )

        for col in df.columns[:-1]:
            df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # df.to_csv(
        #     "./inmet_dados_historicos_curitiba/02.date_time_merged/inmet_curitiba_2003_2025.csv",
        #     index=False,
        # )

        # df_with_real_values = df[~df.eq(-9999).any(axis=1)]
        # print(df_with_real_values.head())

        col_df_list = split_into_frames(df)
        # [print(col_df.head) for col_df in col_df_list]


if __name__ == "__main__":
    main()
