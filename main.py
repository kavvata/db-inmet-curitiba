import glob
import os
import threading

from scripts.csv import normalize_missing
from scripts.db import load_csv_to_db


def load_db():
    thread_list: list[threading.Thread] = []

    csv_dir_path = "./inmet_dados_historicos_curitiba/04.names_for_tables/"
    csv_paths = glob.glob(os.path.join(csv_dir_path, "*.csv"))
    for p in csv_paths:
        thread = threading.Thread(
            target=load_csv_to_db,
            args=(p,),
        )
        thread_list.append(thread)
        print(f"starting work on {p}")
        thread.start()

    for t in thread_list:
        t.join()

    print("threads done!")


def normalize():
    thread_list: list[threading.Thread] = []
    csv_dir_path = "./inmet_dados_historicos_curitiba/04.names_for_tables/"
    csv_paths = glob.glob(os.path.join(csv_dir_path, "*.csv"))
    for p in csv_paths:
        thread = threading.Thread(
            target=normalize_missing,
            args=(p,),
        )
        thread_list.append(thread)
        print(f"starting work on {p}")
        thread.start()


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Weather prediction of Curitiba with scylladb and pytorch."
    )
    parser.add_argument(
        "--load", action="store_true", help="Loads csv data to scylladb"
    )

    parser.add_argument("--work", action="store_true", help="do some.")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if args.load:
        load_db()
    if args.work:
        normalize()
