import requests
import datetime
import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from tqdm import tqdm


class DataDownloader:
    """
    Clase para descargar datos de la API de manera eficiente con manejo de errores.
    
    Args:
        dataset_id: ID del dataset a descargar
        base_url: URL base de la API
        data_dir: Directorio donde se guardarán los archivos
        timeout: Tiempo de espera inicial en segundos
        max_retries: Número máximo de reintentos por petición
        retry_delay: Tiempo de espera base entre reintentos en segundos
        request_delay: Tiempo de espera entre peticiones diferentes
    """
    
    def __init__(
        self,
        dataset_id: str = "972263",
        base_url: str = "https://www.simem.co/backend-files/api/PublicData",
        data_dir: Union[str, Path] = "data",
        timeout: int = 60,
        max_retries: int = 2,
        retry_delay: int = 10,
        request_delay: int = 1
    ):
        self.dataset_id = dataset_id
        self.base_url = base_url
        self.data_dir = Path(data_dir)
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay
        
        # Asegurar que el directorio de datos existe
        self.ensure_data_dir()
    
    def ensure_data_dir(self) -> None:
        """Asegurar que el directorio de datos existe."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def is_date_processed(self, date: datetime.date) -> bool:
        """Verificar si ya existe un archivo para la fecha especificada."""
        date_str = date.strftime("%Y-%m-%d")
        filename = self.data_dir / f"data_{date_str}.json"
        return filename.exists()
    
    def download_data(self, date: datetime.date) -> Optional[Dict[str, Any]]:
        """
        Descargar datos para una fecha específica con lógica de reintento.
        
        Args:
            date: Fecha para la cual descargar datos
            
        Returns:
            Diccionario con los datos o None si hubo un error
        """
        date_str = date.strftime("%Y-%m-%d")
        params = {
            'datasetId': self.dataset_id,
            'startDate': date_str
        }
        
        for attempt in range(self.max_retries):
            timeout = self.timeout * (attempt + 1)
            try:
                response = requests.get(
                    self.base_url,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                if attempt == self.max_retries - 1:
                    logging.error(f"Timeout after {self.max_retries} attempts for {date}")
                    return None
                wait_time = self.retry_delay * (attempt + 1)
                logging.warning(
                    f"Timeout for {date}, retrying in {wait_time} seconds... "
                    f"(Attempt {attempt + 1}/{self.max_retries})"
                )
                time.sleep(wait_time)
            except (requests.RequestException, ValueError) as e:
                logging.error(f"Error downloading data for {date}: {e}")
                return None
        
        return None
    
    def save_data(self, date: datetime.date, data: Dict[str, Any]) -> bool:
        """
        Guardar los datos descargados en un archivo.
        
        Args:
            date: Fecha de los datos
            data: Datos a guardar
            
        Returns:
            True si se guardó correctamente, False en caso contrario
        """
        date_str = date.strftime("%Y-%m-%d")
        filename = self.data_dir / f"data_{date_str}.json"
        
        try:
            # Extraer solo la parte relevante de los datos
            data_to_save = data.get('result', {}).get('records', [])
            with open(filename, 'w') as f:
                json.dump(data_to_save, f, indent=2)
            return True
        except (IOError, AttributeError, TypeError) as e:
            logging.error(f"Error saving data for {date_str}: {e}")
            return False
    
    def process_date_range(
        self,
        start_date: datetime.date,
        end_date: datetime.date
    ) -> Dict[str, List[str]]:
        """
        Procesa un rango de fechas, descargando y guardando los datos.
        
        Args:
            start_date: Fecha de inicio
            end_date: Fecha de fin
            
        Returns:
            Diccionario con listas de fechas procesadas, fallidas y saltadas
        """
        current_date = start_date
        processed_dates = []
        failed_dates = []
        skipped_dates = []
        
        total_days = (end_date - start_date).days + 1
        
        with tqdm(total=total_days, desc="Downloading data", unit="day") as pbar:
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                
                if self.is_date_processed(current_date):
                    pbar.set_postfix_str(f"Skipped {date_str}")
                    skipped_dates.append(date_str)
                    current_date += datetime.timedelta(days=1)
                    pbar.update(1)
                    continue
                    
                pbar.set_postfix_str(f"Processing {date_str}")
                data = self.download_data(current_date)
                
                if data and self.save_data(current_date, data):
                    pbar.set_postfix_str(f"Saved {date_str}")
                    processed_dates.append(date_str)
                else:
                    pbar.set_postfix_str(f"Failed {date_str}")
                    failed_dates.append(date_str)
                
                time.sleep(self.request_delay)
                current_date += datetime.timedelta(days=1)
                pbar.update(1)
        
        # Imprimir resumen
        print("\n" + "="*50)
        print(f"Processing complete!")
        print(f"Total days: {total_days}")
        print(f"Successfully processed: {len(processed_dates)}")
        print(f"Skipped (already exists): {len(skipped_dates)}")
        print(f"Failed: {len(failed_dates)}")
        if failed_dates:
            print(f"Failed dates: {', '.join(failed_dates)}")
        print("="*50)
        
        return {
            'processed': processed_dates,
            'failed': failed_dates,
            'skipped': skipped_dates
        }

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_downloader.log',
    filemode='a'
)

def main():
    """Función principal para ejecutar el script directamente."""
    # Crear una instancia del descargador
    downloader = DataDownloader(
        dataset_id="972263",
        base_url="https://www.simem.co/backend-files/api/PublicData",
        data_dir="data",
        timeout=60,
        max_retries=2,
        retry_delay=10,
        request_delay=1
    )
    
    # Fechas de ejemplo (puedes modificarlas según necesites)
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2025, 5, 30)
    
    try:
        # Procesar el rango de fechas
        results = downloader.process_date_range(start_date, end_date)
        logging.info("Processing completed successfully.")
        return results
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
