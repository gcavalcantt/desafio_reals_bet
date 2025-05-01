import os
import pickle
import pandas as pd

def carregar_dados_csv(pasta_dados: str, arquivos_csv: list) -> dict:
    """
    Carrega arquivos CSV para DataFrames e retorna um dicionário.
    
    Args:
        pasta_dados: Caminho da pasta contendo os arquivos
        arquivos_csv: Lista de nomes de arquivos CSV
        
    Returns:
        Dicionário com {nome_do_arquivo: DataFrame}
    """
    tabelas = {}
    
    for arquivo in arquivos_csv:
        caminho = os.path.join(pasta_dados, arquivo)
        
        if not os.path.exists(caminho):
            print(f"⚠ Arquivo não encontrado: {arquivo}")
            continue
            
        nome_df = os.path.splitext(arquivo)[0]
        
        try:
            # Tentativa com UTF-8
            tabelas[nome_df] = pd.read_csv(caminho, sep=',', encoding='utf-8')
            print(f"✔ {arquivo} lido com UTF-8")
            
        except UnicodeDecodeError:
            try:
                # Fallback para Latin-1
                tabelas[nome_df] = pd.read_csv(caminho, sep=',', encoding='latin1')
                print(f"✔ {arquivo} lido com Latin-1")
                
            except Exception as e:
                print(f"❌ Falha ao ler {arquivo}: {str(e)}")
                continue
                
        except Exception as e:
            print(f"❌ Erro inesperado com {arquivo}: {str(e)}")
            continue
    
    return tabelas

def carregar_excel(pasta_dados: str, nome_arquivo: str) -> pd.DataFrame:
    """
    Carrega um arquivo Excel como DataFrame.
    
    Args:
        pasta_dados: Caminho da pasta contendo o arquivo
        nome_arquivo: Nome do arquivo Excel (com extensão)
        
    Returns:
        DataFrame com os dados ou None em caso de falha
    """
    caminho = os.path.join(pasta_dados, nome_arquivo)
    
    if not os.path.exists(caminho):
        print(f"Erro: Arquivo não encontrado em {caminho}")
        return None
    
    try:
        df = pd.read_excel(caminho)
        print(f"✔ {nome_arquivo} lido com sucesso: {df.shape[0]} linhas, {df.shape[1]} colunas")
        return df
    except Exception as e:
        print(f"❌ Falha ao ler {nome_arquivo}: {str(e)}")
        return None

def main():
    # Configuração de caminhos
    base_dir = os.path.dirname(os.path.abspath(__file__))
    pasta_dados = os.path.abspath(os.path.join(base_dir, '..', 'banco_uxbg'))
    
    # Lista de arquivos CSV
    arquivos_csv = [
        'agencias.csv',
        'colaborador_agencia.csv',
        'colaboradores.csv',
        'contas.csv',
        'propostas_credito.csv',
        'transacoes.csv'
    ]
    
    # Carrega dados
    tabelas = carregar_dados_csv(pasta_dados, arquivos_csv)
    
    # Adiciona dados do Excel
    clientes_df = carregar_excel(pasta_dados, 'clientes.xlsx')
    if clientes_df is not None:
        tabelas['clientes'] = clientes_df
    
    # Verificação final
    print("\nDataFrames carregados:")
    for nome, df in tabelas.items():
        print(f"- {nome}: {df.shape[0]} linhas, {df.shape[1]} colunas")
    
    # Salva os dados em pickle
    with open('dados_extraidos.pkl', 'wb') as f:
        pickle.dump(tabelas, f)
    
    return tabelas

if __name__ == "__main__":
    main()