import os
import pandas as pd

# Caminho para os dados intermediários (salvos no transform.ipynb)
data_folder = r'c:/Users/guilh/OneDrive/Documentos/GitHub/desafio_reals_bet/src/dados_transformados'

# Verificar se os arquivos existem antes de carregar
if not os.path.exists(data_folder):
    raise FileNotFoundError(f"Pasta '{data_folder}' não encontrada!")

# Ler os arquivos transformados
agencias = pd.read_pickle(os.path.join(data_folder, 'agencias.pkl'))
clientes = pd.read_pickle(os.path.join(data_folder, 'clientes.pkl'))
colaborador_agencia = pd.read_pickle(os.path.join(data_folder, 'colaborador_agencia.pkl'))
colaboradores = pd.read_pickle(os.path.join(data_folder, 'colaboradores.pkl'))
contas = pd.read_pickle(os.path.join(data_folder, 'contas.pkl'))
propostas_credito = pd.read_pickle(os.path.join(data_folder, 'propostas_credito.pkl'))
transacoes = pd.read_pickle(os.path.join(data_folder, 'transacoes.pkl'))

# Função para salvar em CSV
def salvar_csv(df, nome_arquivo):
    caminho = os.path.join(data_folder, f'{nome_arquivo}.csv')
    
    # Verifica se o arquivo já existe e sobrescreve
    if os.path.exists(caminho):
        print(f"Arquivo {caminho} já existe, será sobrescrito.")
    
    # Salva o arquivo em formato CSV
    df.to_csv(caminho, index=False)
    print(f"Arquivo {caminho} salvo com sucesso.")

# Salvar todos os dataframes em CSV
salvar_csv(agencias, 'agencias_tratado')
salvar_csv(clientes, 'clientes_tratado')
salvar_csv(colaborador_agencia, 'colaborador_agencia_tratado')
salvar_csv(colaboradores, 'colaboradores_tratado')
salvar_csv(contas, 'contas_tratado')
salvar_csv(propostas_credito, 'propostas_credito_tratado')
salvar_csv(transacoes, 'transacoes_tratado')