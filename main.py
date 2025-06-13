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
        for record in company_records:
            all_records.append(record)
    
    # Configurar y actualizar base de datos
    conn, cursor = create_db_connection()
    try:
        setup_database(cursor)
        conn.commit()
        
        # Procesar registros
        inserted, existing = process_records(cursor, all_records)
        conn.commit()
        
        # Mostrar resumen con formato mejorado
        print("\n" + "="*60)
        print("📊 RESUMEN DE PROCESAMIENTO DE REGISTROS".center(60))
        print("="*60)
        print(f"{'Total de registros procesados:':<40} {len(all_records):>20,}")
        print(f"{'Nuevos registros insertados:':<40} {inserted:>20,}")
        print(f"{'Registros existentes:':<40} {existing:>20,}")
        print("-"*60)
        
        if inserted > 0:
            print("✅ PROCESO COMPLETADO: Se insertaron nuevos registros exitosamente".center(60))
        else:
            print("ℹ️  INFORMACIÓN: No se insertaron registros nuevos (todos los registros ya existían)".center(60))
        print("="*60)
            
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
        
        records = cursor.fetchall()
        total_records = len(records)
        updated = 0
        
        print("\n" + "="*60)
        print("🔄 ACTUALIZACIÓN DE ANÁLISIS DE MERCADO".center(60))
        print("="*60)
        
        if total_records == 0:
            print("ℹ️  No hay registros pendientes de actualizar".center(60))
            print("="*60)
            return
            
        print(f"{'Registros a actualizar:':<40} {total_records:>20,}")
        print("-"*60)
        
        # Configurar la barra de progreso
        progress_bar = tqdm(
            records,
            desc="Actualizando registros",
            unit="registro",
            ncols=80,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
            leave=False
        )
        
        for record in progress_bar:
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
            
            # Actualizar la descripción de la barra de progreso
            progress_bar.set_postfix({"Actualizados": f"{updated}/{total_records}"})
        
        conn.commit()
        print("\n" + "="*60)
        print("🔄 ACTUALIZACIÓN DE ANÁLISIS DE MERCADO".center(60))
        print("="*60)
        print(f"{'Registros actualizados:':<40} {updated:>20,}")
        print("✅ PROCESO DE ACTUALIZACIÓN COMPLETADO CON ÉXITO".center(60))
        print("="*60)
    except Exception as e:
        conn.rollback()
        print("\n" + "="*60)
        print("❌ ERROR EN LA ACTUALIZACIÓN DE REGISTROS".center(60))
        print("="*60)
        print(f"Error: {e}")
        print("="*60)
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_records()
    update_market_analysis_records()
