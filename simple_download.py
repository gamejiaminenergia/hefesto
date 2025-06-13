import requests
import json
from datetime import datetime, date, timedelta
from pathlib import Path

def download_data(date, output_dir='data'):
    """
    Descarga datos para una fecha específica y los guarda en un archivo JSON.
    
    Args:
        date (datetime.date): Fecha para la cual descargar datos
        output_dir (str): Directorio donde guardar los archivos
    """
    # Crear directorio si no existe
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Verificar si el archivo ya existe
    filename = Path(output_dir) / f"data_{date.strftime('%Y-%m-%d')}.json"
    if filename.exists():
        print(f"El archivo {filename} ya existe, omitiendo descarga para {date}")
        return True
        
    # Configurar la URL y parámetros
    url = "https://www.simem.co/backend-files/api/PublicData"
    params = {
        'datasetId': '972263',
        'startDate': date.strftime('%Y-%m-%d'),
        'endDate': date.strftime('%Y-%m-%d')
    }
    
    try:
        print(f"Descargando datos para {date}...")
        response = requests.get(url, params=params, timeout=60*10)
        print(f"URL: {response.url}")  # Debug: Mostrar la URL completa
        response.raise_for_status()
        
        # Obtener los datos de la respuesta
        response_data = response.json()['result']['records']
        
        # Verificar si hay datos en la respuesta
        if not response_data:
            print(f"No se encontraron datos para la fecha {date}")
            return False
            
        # Guardar los datos en un archivo
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(response_data, f, indent=2, ensure_ascii=False)
        print(f"Datos guardados en {filename}")
        return True
            
    except requests.exceptions.RequestException as e:
        print(f"Error en la petición para {date}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error al decodificar la respuesta JSON para {date}: {e}")
    except Exception as e:
        print(f"Error inesperado para {date}: {str(e)}")
    return False

def main():
    # Descargar datos para enero a mayo de 2025
    start_date = date(2025, 1, 1)
    end_date = date(2025, 5, 30)
    
    current_date = start_date
    while current_date <= end_date:
        download_data(current_date)
        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()
