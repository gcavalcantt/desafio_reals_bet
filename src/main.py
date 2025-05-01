import os
import sys
from pathlib import Path
import subprocess
from datetime import datetime

# Configuração de caminhos
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

def configurar_ambiente():
    """Cria diretórios necessários"""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    print(f"✅ Diretórios configurados em: {DATA_DIR}")

def executar_etl():
    """Orquestração do pipeline com proteção contra loops"""
    try:
        inicio = datetime.now()
        print(f"\n⏳ Iniciando ETL em {inicio.strftime('%H:%M:%S')}")
        
        # Variável de controle
        if os.getenv("ETL_EXECUTING") == "1":
            raise RuntimeError("Tentativa de execução recursiva detectada!")
        
        # Marcar como "em execução"
        os.environ["ETL_EXECUTING"] = "1"

        # Extração
        print("\n🔍 Fase de Extração...")
        subprocess.run([sys.executable, str(BASE_DIR / "extract.py"), 
                      "--output", str(RAW_DATA_DIR)], check=True)
        
        # Transformação
        print("\n🔄 Fase de Transformação...")
        subprocess.run([sys.executable, str(BASE_DIR / "transform.py"),
                      "--input", str(RAW_DATA_DIR / "dados_extraidos.pkl"),
                      "--output", str(PROCESSED_DATA_DIR)], check=True)
        
        # Carga
        print("\n📤 Fase de Carga...")
        subprocess.run([sys.executable, str(BASE_DIR / "load.py"),
                      "--input", str(PROCESSED_DATA_DIR)], check=True)
        
        duracao = datetime.now() - inicio
        print(f"\n🎉 Concluído em {duracao.total_seconds():.2f}s")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Falha no subprocesso: {e.stderr or str(e)}")
        sys.exit(1)
    finally:
        # Limpar variável de controle
        os.environ.pop("ETL_EXECUTING", None)

if __name__ == "__main__":
    configurar_ambiente()
    executar_etl()