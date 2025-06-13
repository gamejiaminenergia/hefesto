import json
from pathlib import Path
import pandas as pd
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_batch

def calculate_market_analysis(prompt, system_template):
    return "xxxxxxxx"
    
def create_db_connection() -> tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
    """Crea y retorna una conexión a la base de datos PostgreSQL."""
    def get_connection_url() -> str:
        """Genera la URL de conexión a PostgreSQL desde variables de entorno."""
        load_dotenv()
        user = os.getenv('POSTGRES_USER', 'postgres')
        password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', 5432)
        db = os.getenv('POSTGRES_DB', 'postgres')
        return f'postgresql://{user}:{password}@{host}:{port}/{db}'
    
    conn = psycopg2.connect(get_connection_url())
    return conn, conn.cursor()


def load_json_file(file_path: str) -> dict:
    """Carga un archivo JSON y retorna su contenido como diccionario."""
    with Path(file_path).open('r', encoding='utf-8') as f:
        return json.load(f)


def process_company_data(company_data: dict, template: dict) -> List[Dict[str, Any]]:
    """Procesa los datos de una compañía según el template proporcionado."""
    df = pd.json_normalize(
        data=template,
        record_path=["assigned_prompts"],
        meta=["company_name", "company_code", "system_id", 
              "system_name", "system_description", "system_template"]
    )

    # Aplicar formato a las columnas
    df["company_name"] = df["company_name"].apply(
        lambda x: x.format(company_name=company_data["company_name"]))
    df["company_code"] = df["company_code"].apply(
        lambda x: x.format(company_code=company_data["company_code"]))
    df["prompt"] = df["prompt"].apply(
        lambda x: x.format(company_name=company_data["company_name"]))
    
    # Añadir columnas adicionales
    df = df.assign(
        market_analysis="",
        score_market_analysis=0,
        status=0,
        processed=False
    )
    
    return df.to_dict(orient="records")


def setup_database(cursor) -> None:
    """Configura la base de datos creando la tabla e índices necesarios."""
    # Crear tabla si no existe
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test (
            id SERIAL PRIMARY KEY,
            info JSONB
        );
    """)

    # Crear índice único si no existe
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE indexname = 'idx_test_unique_prompt'
            ) THEN
                CREATE UNIQUE INDEX idx_test_unique_prompt 
                ON test ((info->>'company_code'), (info->>'prompt_id'));
            END IF;
        END $$;
    """)


def process_records(cursor, records: List[Dict[str, Any]]) -> tuple[int, int]:
    """Procesa los registros, insertando solo los que no existen.
    
    Retorna:
        tuple: (inserted, existing) - cantidad de registros insertados y existentes
    """
    inserted = 0
    existing = 0
    
    for record in tqdm(records, desc="Processing records"):
        company_code = record["company_code"]
        prompt_id = str(record["prompt_id"])

        # Verificar si ya existe
        cursor.execute("""
            SELECT 1 FROM test 
            WHERE info->>'company_code' = %s AND (info->>'prompt_id')::text = %s
            LIMIT 1;
        """, (company_code, prompt_id))
        
        if cursor.fetchone():
            existing += 1
        else:
            cursor.execute("INSERT INTO test (info) VALUES (%s)", [json.dumps(record)])
            inserted += 1
    
    return inserted, existing

def create_records():
    # Cargar datos
    template = load_json_file("data/market_analysis_template.json")
    companies = load_json_file("data/companies.json")
    
    # Procesar todas las compañías
    all_records = []
    for company_data in companies:
        company_records = process_company_data(company_data, template)
        for record in tqdm(company_records, 
                         desc=f"Processing {company_data['company_code']}"):
            all_records.append(record)
    
    # Configurar y actualizar base de datos
    conn, cursor = create_db_connection()
    try:
        setup_database(cursor)
        conn.commit()
        
        # Procesar registros
        inserted, existing = process_records(cursor, all_records)
        conn.commit()
        
        # Mostrar resumen
        print(f"\n📊 Resumen de la ejecución:")
        print(f"- Total de registros procesados: {len(all_records)}")
        print(f"- Nuevos registros insertados: {inserted}")
        print(f"- Registros existentes: {existing}")
        
        if inserted > 0:
            print("✅ Proceso completado con éxito")
        else:
            print("ℹ️ No se insertaron nuevos registros (todos los registros ya existían)")
            
    finally:
        cursor.close()
        conn.close()

def update_market_analysis_records():
    conn, cursor = create_db_connection()
    try:
        # Obtener registros no procesados
        cursor.execute("""
            SELECT id, info->>'prompt' as prompt, info->>'system_template' as system_template 
            FROM test 
            WHERE (info->>'status')::int = 0
        """)
        
        updated = 0
        for record in cursor.fetchall():
            record_id, prompt, system_template = record
            # Calcular el nuevo market_analysis
            new_analysis = calculate_market_analysis(prompt, system_template)
            
            # Actualizar el registro
            cursor.execute("""
                UPDATE test 
                SET info = jsonb_set(
                    jsonb_set(
                        info,
                        '{market_analysis}',
                        to_jsonb(%s::text)
                    ),
                    '{status}',
                    '1'::jsonb
                )
                WHERE id = %s
            """, (new_analysis, record_id))
            updated += 1
        
        conn.commit()
        print(f"✅ {updated} registros actualizados con éxito")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error al actualizar registros: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_records()
    update_market_analysis_records()
