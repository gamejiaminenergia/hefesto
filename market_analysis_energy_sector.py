import os
import sqlite3
from pathlib import Path
from typing import List, Optional

import pandas as pd


class EnergyMarketAnalysis:
    """Clase para el análisis de mercado del sector energético."""
    
    def __init__(self, data_dir: str = 'data'):
        """Inicializa el análisis con el directorio de datos.
        
        Args:
            data_dir: Ruta al directorio que contiene los archivos JSON de entrada.
        """
        self.data_dir = Path(data_dir)
        self.df_companies: Optional[pd.DataFrame] = None
        self.db_path = 'energy_market_analysis.db'
        self.table_name = 'market_analysis'
    
    def load_energy_companies(self) -> None:
        """Carga los datos de empresas del sector energético desde archivos JSON."""
        try:
            json_files = list(self.data_dir.glob('*.json'))
            if not json_files:
                raise FileNotFoundError(f'No se encontraron archivos JSON en {self.data_dir}')
                
            self.df_companies = pd.concat([
                pd.read_json(file_path) 
                for file_path in json_files
            ], ignore_index=True)
            print(f'Se cargaron {len(self.df_companies)} registros de empresas')
            
        except Exception as e:
            print(f'Error al cargar los datos de empresas: {e}')
            raise
    
    def analyze_market_structure(self) -> None:
        """Analiza la estructura del mercado basado en las actividades de las empresas."""
        if self.df_companies is None:
            raise ValueError('No hay datos de empresas cargados')
        
        try:
            # Eliminar duplicados
            initial_count = len(self.df_companies)
            self.df_companies = self.df_companies.drop_duplicates(
                subset='CodigoSICAgente', 
                keep='last'
            )
            print(f'Registros únicos: {len(self.df_companies)} (se eliminaron {initial_count - len(self.df_companies)} duplicados)')
            
            # Extraer código de empresa (primeros 3 caracteres)
            self.df_companies['codigo_empresa'] = self.df_companies['CodigoSICAgente'].astype(str).str[:3]
            self.df_companies['actividad'] = self.df_companies['ActividadAgente']
            self.df_companies['nombre_empresa'] = self.df_companies['NombreAgente']
            
            # Analizar distribución de actividades
            activity_distribution = self.df_companies['actividad'].value_counts()
            print('\nDistribución de actividades en el mercado:')
            print(activity_distribution)
            
            # Crear tabla pivote para análisis de mercado
            self.market_analysis = self.df_companies.pivot_table(
                index=['codigo_empresa', 'nombre_empresa'], 
                columns='actividad', 
                aggfunc='size', 
                fill_value=0
            ).reset_index()
            
            # Calcular métricas de mercado
            self._calculate_market_metrics()
            
        except Exception as e:
            print(f'Error al analizar la estructura del mercado: {e}')
            raise
    
    def _calculate_market_metrics(self) -> None:
        """Calcula métricas clave del mercado."""
        # Número total de empresas
        total_companies = len(self.market_analysis)
        
        # Contar empresas por tipo de actividad
        activity_columns = [col for col in self.market_analysis.columns 
                          if col not in ['codigo_empresa', 'nombre_empresa']]
        
        print('\nMétricas del mercado:')
        for activity in activity_columns:
            count = (self.market_analysis[activity] > 0).sum()
            print(f'- Empresas con actividad {activity}: {count} ({(count/total_companies*100):.1f}%)')
    
    def save_market_analysis(self) -> None:
        """Guarda el análisis de mercado en una base de datos SQLite."""
        if not hasattr(self, 'market_analysis') or self.market_analysis.empty:
            raise ValueError('No hay datos de análisis para guardar')
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                self.market_analysis.to_sql(
                    name=self.table_name,
                    con=conn,
                    if_exists='replace',
                    index=False
                )
            print(f'\nAnálisis de mercado guardado exitosamente en {self.db_path}')
            
        except Exception as e:
            print(f'Error al guardar el análisis: {e}')
            raise
    
    def build_company_analysis(self) -> None:
        """Construye y analiza la tabla de compañías del sector energético."""
        try:
            print('=== Análisis del Mercado Energético ===')
            
            print('\n1. Cargando datos de empresas...')
            self.load_energy_companies()
            
            print('\n2. Analizando estructura del mercado...')
            self.analyze_market_structure()
            
            print('\n3. Guardando resultados...')
            self.save_market_analysis()
            
            print('\n=== Tabla de compañías generada exitosamente ===')
            
        except Exception as e:
            print(f'\nError al construir el análisis de compañías: {e}')
            raise


def main():
    """Función principal para construir el análisis de compañías del mercado."""
    try:
        # Crear instancia y construir el análisis
        analyzer = EnergyMarketAnalysis()
        analyzer.build_company_analysis()
        return 0
    except Exception as e:
        print(f'Error en la ejecución: {e}')
        return 1


if __name__ == '__main__':
    exit(main())
