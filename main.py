import glob
import os
import threading

from scripts.load_into_database import load_csv_to_db


thread_list: list[threading.Thread] = []


def main():
    csv_dir_path = "./inmet_dados_historicos_curitiba/04.names_for_tables/"
    print("running main...")
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


if __name__ == "__main__":
    main()
