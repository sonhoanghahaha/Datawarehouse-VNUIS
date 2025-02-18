from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# Thông tin dự án và dataset
PROJECT_ID = "dtwh-final"
SOURCE_DATASET = "son_test"
TARGET_DATASET = "NewGame_dataset"

# Default arguments cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
with DAG(
    dag_id="create_dim_fact_tables",
    default_args=default_args,
    schedule_interval="@daily",  # Chạy 1 lần mỗi ngày
    catchup=False,
) as dag:

    # Tạo bảng Dim_Customers
    create_dim_customers = BigQueryInsertJobOperator(
        task_id="create_dim_customers",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Dim_Customers` AS
                    SELECT 
                      c.customer_id,
                      CONCAT(c.first_name, ' ', c.last_name) AS full_name,
                      c.birthdate,
                      c.email,
                      c.phone,
                      a.city,
                      a.country,
                      c.registration_date,
                      CASE WHEN c.VIP IS NOT NULL THEN TRUE ELSE FALSE END AS is_vip
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.customers` c
                    LEFT JOIN `{PROJECT_ID}.{SOURCE_DATASET}.addresses` a
                    ON c.address_id = a.address_id
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tạo bảng Dim_Addresses
    create_dim_addresses = BigQueryInsertJobOperator(
        task_id="create_dim_addresses",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Dim_Addresses` AS
                    SELECT 
                      address_id,
                      address,
                      city,
                      country,
                      postal_code
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.addresses`
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tạo bảng Dim_Staff
    create_dim_staff = BigQueryInsertJobOperator(
        task_id="create_dim_staff",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Dim_Staff` AS
                    SELECT 
                      s.staff_id,
                      CONCAT(s.first_name, ' ', s.last_name) AS full_name,
                      s.salary,
                      s.birthdate,
                      s.start AS start_date,
                      s.phone,
                      a.city,
                      a.country,
                      s.email
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.staff` s
                    LEFT JOIN `{PROJECT_ID}.{SOURCE_DATASET}.addresses` a
                    ON s.address_id = a.address_id
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tạo bảng Dim_Games
    create_dim_games = BigQueryInsertJobOperator(
        task_id="create_dim_games",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Dim_Games` AS
                    SELECT 
                      g.game_id,
                      g.name AS game_name,
                      g.category,
                      g.rating,
                      g.duration,
                      g.min_age,
                      g.min_players,
                      g.max_players,
                      g.tournaments,
                      g.max_players_in_team,
                      g.min_players_in_team
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.games` g
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tạo bảng Dim_Tournaments
    create_dim_tournaments = BigQueryInsertJobOperator(
        task_id="create_dim_tournaments",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Dim_Tournaments` AS
                    SELECT 
                      t.tournament_id,
                      t.name AS tournament_name,
                      t.date AS tournament_date,
                      t.game_id,
                      t.team_players_number,
                      t.total_players_number,
                      CONCAT(s.first_name, ' ', s.last_name) AS organizer_name
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.tournament` t
                    LEFT JOIN `{PROJECT_ID}.{SOURCE_DATASET}.staff` s
                    ON t.staff_id = s.staff_id
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tạo bảng Fact_Inventory
    create_fact_inventory = BigQueryInsertJobOperator(
        task_id="create_fact_inventory",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Fact_Inventory` AS
                    SELECT 
                      b.inventory_id,
                      b.game_id,
                      b.price,
                      b.available,
                      'buy' AS transaction_type
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.inventory_buy` b
                    UNION ALL
                    SELECT 
                      r.inventory_id,
                      r.game_id,
                      r.price,
                      r.available,
                      'rent' AS transaction_type
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.inventory_rent` r
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tạo bảng Fact_Transactions
    create_fact_transactions = BigQueryInsertJobOperator(
        task_id="create_fact_transactions",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Fact_Transactions` AS
                    SELECT 
                      p.purchase_id AS transaction_id,
                      p.inventory_id,
                      p.customer_id,
                      p.staff_id,
                      p.date AS transaction_date,
                      NULL AS return_date,
                      NULL AS fine,
                      b.price AS transaction_price,
                      'purchase' AS transaction_type
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.purchases` p
                    LEFT JOIN `{PROJECT_ID}.{SOURCE_DATASET}.inventory_buy` b
                    ON p.inventory_id = b.inventory_id
                    UNION ALL
                    SELECT 
                      r.rental_id AS transaction_id,
                      r.inventory_id,
                      r.customer_id,
                      r.staff_id,
                      r.rental_date AS transaction_date,
                      r.return_date,
                      r.fine,
                      r.price AS transaction_price,
                      'rental' AS transaction_type
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.rentals` r
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tạo bảng Fact_Tournament_Results
    create_fact_tournament_results = BigQueryInsertJobOperator(
        task_id="create_fact_tournament_results",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.Fact_Tournament_Results` AS
                    SELECT 
                      tr.tournament_id,
                      tr.customer_id,
                      tr.place,
                      tr.score,
                      t.game_id
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.tournament_results` tr
                    LEFT JOIN `{PROJECT_ID}.{SOURCE_DATASET}.tournament` t
                    ON tr.tournament_id = t.tournament_id
                """,
                "useLegacySql": False,
            }
        },
    )

    # Sắp xếp thứ tự chạy
    create_dim_customers >> create_dim_addresses >> create_dim_staff >> create_dim_games >> create_dim_tournaments
    create_dim_games >> create_fact_inventory >> create_fact_transactions >> create_fact_tournament_results