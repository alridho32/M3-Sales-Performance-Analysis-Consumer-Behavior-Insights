import pandas as pd
import psycopg2 as db
import datetime as dt
import re
from datetime import timedelta
import warnings
warnings.filterwarnings("ignore")

from airflow.models import DAG
from elasticsearch import Elasticsearch
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

'''
=================================================
Milestone 3

Nama  : Achmed Alridho Zulkarnaen
Batch : FTDS-037-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang digunakan berkaitan dengan 
penjualan di supermarket, yang mencakup informasi transaksi seperti ID invoice, cabang, kota, tipe pelanggan, produk yang dibeli, harga per unit, jumlah 
pembelian, pajak, total transaksi, metode pembayaran, dan margin keuntungan. Data ini memberikan wawasan tentang perilaku konsumen, tren produk, dan faktor-faktor 
yang memengaruhi keputusan pembelian, yang dapat digunakan untuk analisis penjualan dan pengelolaan strategi pemasaran.
=================================================
'''
'''
Latar Belakang:

Industri supermarket saat ini dihadapkan pada tantangan besar dalam memahami pola perilaku konsumen 
guna meningkatkan penjualan dan pengalaman berbelanja. Data transaksi yang mencakup berbagai informasi 
seperti produk, harga, jumlah pembelian, dan ulasan pelanggan memberikan peluang bagi perusahaan untuk 
menganalisis faktor-faktor yang mempengaruhi keputusan pembelian. Dengan pemahaman yang lebih mendalam 
tentang pola konsumsi, perusahaan dapat merancang strategi pemasaran yang lebih tepat, mengelola stok 
secara lebih efisien, dan meningkatkan relevansi produk yang ditawarkan kepada konsumen.

Tujuan:

Sebagai Data Analyst di perusahaan supermarket X, saya bertujuan untuk menganalisis data penjualan 
guna memberikan wawasan yang dapat mendukung strategi pemasaran dan pengelolaan produk yang lebih 
baik. Fokus utama saya adalah mengidentifikasi produk-produk dengan penjualan terbaik, menganalisis 
hubungan antara metode pembayaran dan volume transaksi, serta mengungkap tren pembelian berdasarkan 
kategori produk. Hasil analisis ini diharapkan dapat membantu tim pemasaran dan manajemen supermarket X 
dalam merencanakan langkah-langkah yang lebih efektif untuk meningkatkan penjualan dan memperbaiki 
pengalaman pelanggan.

User: 

Tim pemasaran dan manajemen supermarket X.
'''

# Mengambil data dari database PostgreSQL.
def fetch_data():
    '''
    Fungsi ini digunakan untuk mengambil data pada postgres dengan bantuan query, 
    kemudian data akan disimpan dalam bentuk csv.
    '''
    connection = db.connect(
            database = 'airflow',
            user = 'airflow',
            password = 'airflow',
            host = 'postgres', 
            port = '5432'             
        )
    # Get all data
    select_query = 'SELECT * FROM table_m3'
    df = pd.read_sql(select_query, connection)
    df.to_csv('/opt/airflow/dags/P2M3_alridho_data_raw.csv', index=False)
    connection.close()

# Cleaning Data
def data_cleaning():
    '''
    Fungsi data_cleaning ini dirancang untuk melakukan pembersihan data dengan beberapa tahapan yang terstruktur. Pada langkah pertama, data akan dimuat terlebih dahulu (yang dalam praktiknya akan disesuaikan dengan kondisi data yang ada). Setelah itu, fungsi ini akan menjalankan empat proses utama untuk memastikan kualitas data yang lebih baik, yaitu:

    1. Menghapus Duplikasi: Menyaring data yang memiliki entri yang sama untuk memastikan bahwa tidak ada duplikasi informasi.
    2. Mengonversi Tipe Data: Mengubah tipe data kolom Date dari objek (string) menjadi tipe datetime, sehingga data tanggal dapat diproses lebih lanjut.
    3. Normalisasi Nama Kolom: Menstandarkan nama kolom agar lebih konsisten dan mudah dibaca.
    4. Penanganan Nilai yang Hilang: Menangani data yang hilang dengan metode yang sesuai, baik dengan pengisian nilai atau penghapusan entri yang tidak relevan.

    Setelah seluruh proses pembersihan selesai, data yang telah dibersihkan akan disimpan dengan nama baru untuk menjaga data asli tetap aman.
    '''
    df = pd.read_csv('/opt/airflow/dags/P2M3_alridho_data_raw.csv')
    # 1. Drop Duplicated
    df = df.drop_duplicates()
    # 2. Mengubah data type kolom Date --> datetime
    def converter(kolom):
        if kolom.name == 'Date':
            return pd.to_datetime(kolom, errors='coerce')
        else:
            return kolom
    df = df.apply(converter)
    # 3. Normalisasi nama kolom
    def normalisasi_kolom(column):
        return re.sub(r'[^a-zA-Z0-9_]', '', column.lower().replace(' ', '_'))
    df.columns = map(normalisasi_kolom, df.columns)
    # 4. Drop missing value
    df = df.dropna()
    # save into_csv
    df.to_csv('/opt/airflow/dags/P2M3_alridho_data_clean.csv', index=True)

# Elasticsearch
def insert_into_elastic():
    '''
    Fungsi insert_into_elastic digunakan untuk mengimpor data ke dalam Elasticsearch dan menghubungkannya dengan Kibana untuk visualisasi. 
    Dengan memasukkan data ke Elasticsearch, fungsi ini memudahkan analisis dan pencarian data secara cepat, sementara Kibana memungkinkan 
    pembuatan visualisasi interaktif untuk memahami data secara lebih baik.
    '''
    df = pd.read_csv('/opt/airflow/dags/P2M3_alridho_data_clean.csv')
    # check connection
    es = Elasticsearch('http://elasticsearch:9200')
    print('Connection status : ', es.ping())
    # insert csv file to elastic search
    # Initialize a list to store failed inserts
    failed_insert = []
    # Iterate through each row in the DataFrame
    for i, r in df.iterrows():
        doc = r.to_json()  # Convert row to JSON
        doc_id = str(r.get('index', i))  # Use the 'index' column or row index as the document ID
        try:
            print(f"Inserting document {i}, target: {r.get('target', 'No target')}")
            # Insert the document using the index as the document ID
            res = es.index(index='index', id=doc_id, doc_type="_doc", body=doc)
        except Exception as e:
            print(f'Index Failed: {r.get("index", "No index")}, Error: {str(e)}')
            failed_insert.append(doc_id)  # Track the failed inserts using document ID
            continue  # Skip to next iteration in case of an error
    print('DONE')
    print('Failed Insert:', failed_insert)


# Data Pipeline
# Proses ini mengatur penjadwalan dengan menggunakan zona waktu Indonesia, dimulai dari tanggal 1 November 2024.
default_args = {
    'owner': 'alridho',
    'start_date': dt.datetime(2024, 11, 1) - dt.timedelta(hours=7),  # Mulai pada 1 Nov 2024, waktu Indonesia
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5)
}
# Mengatur jadwal penjadwalan: Setiap hari Sabtu antara jam 09:10 AM - 09:30 AM dengan interval 10 menit
with DAG(
    "P2M3",
    description='P2M3 ML Pipeline',
    schedule_interval='10-30/10 9 * * 6',  # Setiap hari Sabtu pada interval 10 menit antara 09:10 AM - 09:30 AM
    default_args=default_args,
    catchup=False
) as dag:

# Task untuk memulai proses
    node_start = BashOperator(
    task_id='starting',
    bash_command='echo "Starting the process of reading the CSV file to load the data..."'
)

# Task untuk mengambil data
    node_fetch_data = PythonOperator(
    task_id='fetch-data',
    python_callable=fetch_data
)

# Task untuk membersihkan data
    node_data_cleaning = PythonOperator(
    task_id='data-cleaning',
    python_callable=data_cleaning
)

# Task untuk memasukkan data ke Elasticsearch
    node_insert_data_to_elastic = PythonOperator(
    task_id='insert-data-to-elastic',
    python_callable=insert_into_elastic
)

# Penjadwalan urutan eksekusi task
node_start >> node_fetch_data >> node_data_cleaning >> node_insert_data_to_elastic