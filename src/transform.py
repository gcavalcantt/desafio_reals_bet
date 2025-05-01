import pandas as pd
import pickle
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Final
from types import MappingProxyType

def get_protected_mapping(original: dict) -> MappingProxyType:
    """Retorna uma versão imutável do dicionário"""
    return MappingProxyType(original.copy())

_LOCAL_UF_MAP  = {
    'AC': 'Acre',
    'AL': 'Alagoas',
    'AP': 'Amapá',
    'AM': 'Amazonas',
    'BA': 'Bahia',
    'CE': 'Ceará',
    'DF': 'Distrito Federal',
    'ES': 'Espírito Santo',
    'GO': 'Goiás',
    'MA': 'Maranhão',
    'MT': 'Mato Grosso',
    'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais',
    'PA': 'Pará',
    'PB': 'Paraíba',
    'PR': 'Paraná',
    'PE': 'Pernambuco',
    'PI': 'Piauí',
    'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia',
    'RR': 'Roraima',
    'SC': 'Santa Catarina',
    'SP': 'São Paulo',
    'SE': 'Sergipe',
    'TO': 'Tocantins'
}

_LOCAL_TIPO_CONTA_MAP  = {
    'PF': 'Pessoa Física',
    'PJ': 'Pessoa Jurídica'
}

_LOCAL_TIPO_CLIENTE_MAP  = {
    'PF': 'Pessoa Física',
    'PJ': 'Pessoa Jurídica'
}

# Funções de acesso seguro aos dicionários
def get_uf_map() -> MappingProxyType:
    """Retorna cópia imutável do mapeamento de UFs"""
    return get_protected_mapping(_LOCAL_UF_MAP)

def get_tipo_conta_map() -> MappingProxyType:
    """Retorna cópia imutável do mapeamento de tipos de conta"""
    return get_protected_mapping(_LOCAL_TIPO_CONTA_MAP)

def get_tipo_cliente_map() -> MappingProxyType:
    """Retorna cópia imutável do mapeamento de tipos de cliente"""
    return get_protected_mapping(_LOCAL_TIPO_CLIENTE_MAP)

# Configurações iniciais
def carregar_dados(caminho_pkl: str) -> Dict[str, pd.DataFrame]:
    """Carrega os dados do arquivo pickle"""
    with open(caminho_pkl, 'rb') as f:
        return pickle.load(f)

# Versão ultra-resiliente da função de mapeamento
def aplicar_mapeamento(df: pd.DataFrame, coluna: str, mapeamento) -> pd.DataFrame:
    """
    - Aceita dicionários, MappingProxyType ou callable
    - Conversão segura para dicionário
    - Fallback automático
    """
    if coluna not in df.columns:
        return df

    # Dicionário de fallback genérico
    FALLBACK_MAP = {k: k for k in df[coluna].unique()}
    
    try:
        # Converte para dicionário se necessário
        if callable(mapeamento):
            mapeamento = mapeamento()
        
        if isinstance(mapeamento, (dict, MappingProxyType)):
            safe_map = dict(mapeamento)
        else:
            print(f"⚠️ Tipo inválido: {type(mapeamento)} - Usando fallback")
            safe_map = FALLBACK_MAP
        
        df[coluna] = df[coluna].map(safe_map).fillna(df[coluna])
        
    except Exception as e:
        print(f"⛔ Erro crítico: {str(e)} - Aplicando fallback")
        df[coluna] = df[coluna].map(FALLBACK_MAP).fillna(df[coluna])
    
    return df

def extrair_uf(endereco: str) -> str:
    """Extrai UF de um endereço usando regex"""
    if pd.isna(endereco):
        return None
    
    padroes = [
        r'/\s*([A-Z]{2})\b',              # Padrão 1: "... / RS"
        r'\b([A-Z]{2})\s*\d{5}-?\d{3}$',  # Padrão 2: "RS 90000-000"
        r'\b([A-Z]{2})\s*$'               # Padrão 3: "... RS"
    ]
    
    for padrao in padroes:
        match = re.search(padrao, str(endereco))
        if match:
            return match.group(1).upper()
    return None

def processar_nome_completo(df: pd.DataFrame) -> pd.DataFrame:
    """Combina primeiro_nome e ultimo_nome em nome_completo"""
    if all(col in df.columns for col in ['primeiro_nome', 'ultimo_nome']):
        df['nome_completo'] = df['primeiro_nome'] + ' ' + df['ultimo_nome']
        df.drop(columns=['primeiro_nome', 'ultimo_nome'], inplace=True)
        cols = list(df.columns)
        cols.insert(1, cols.pop(cols.index('nome_completo')))
        return df[cols]
    return df

def calcular_idade(df: pd.DataFrame, col_data: str) -> pd.DataFrame:
    """Calcula idade a partir de data de nascimento"""
    if col_data in df.columns:
        df['idade'] = ((pd.to_datetime('today') - 
                       pd.to_datetime(df[col_data], errors='coerce')).dt.days // 365)
        df.drop(columns=[col_data], inplace=True)
    return df

# Chamadas para usar as funções de acesso
def transformar_clientes(clientes: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para clientes"""
    clientes['uf'] = clientes['endereco'].apply(extrair_uf)
    clientes = aplicar_mapeamento(clientes, 'uf', get_uf_map)  # Agora passando a função
    clientes = aplicar_mapeamento(clientes, 'tipo_cliente', get_tipo_cliente_map)
    clientes = processar_nome_completo(clientes)
    
    # Datas e tempo como cliente
    clientes['data_inclusao'] = pd.to_datetime(clientes['data_inclusao'], errors='coerce').dt.tz_localize(None)
    clientes['tempo_como_cliente'] = (pd.Timestamp.now().tz_localize(None) - clientes['data_inclusao']).dt.days / 30
    
    # Idade
    clientes = calcular_idade(clientes, 'data_nascimento')
    
    return clientes

def transformar_agencias(agencias: pd.DataFrame, contas: pd.DataFrame, transacoes: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para agências"""
    agencias = aplicar_mapeamento(agencias, 'uf', get_uf_map)
    
    # Merge para cálculos
    contas_agencias = pd.merge(contas, agencias, on='cod_agencia')
    transacoes_contas = pd.merge(transacoes, contas, on='num_conta')
    transacoes_agencias = pd.merge(transacoes_contas, agencias, on='cod_agencia')
    
    # Métricas
    agencias['saldo_medio'] = contas_agencias.groupby('nome')['saldo_disponivel'].mean()
    agencias['num_contas'] = contas_agencias.groupby('nome')['num_conta'].count()
    agencias['volume_transacoes'] = transacoes_agencias.groupby('nome')['valor_transacao'].sum()
    
    return agencias

def transformar_transacoes(transacoes: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para transações"""
    transacoes['data_transacao'] = pd.to_datetime(transacoes['data_transacao'], errors='coerce', utc=True)
    transacoes['mes_ano'] = transacoes['data_transacao'].dt.tz_localize(None).dt.to_period('M').astype(str)
    
    # Métricas
    transacoes['total_transacoes_tipo'] = transacoes['nome_transacao'].map(
        transacoes['nome_transacao'].value_counts())
    transacoes['valor_medio'] = transacoes['nome_transacao'].map(
        transacoes.groupby('nome_transacao')['valor_transacao'].mean())
    
    evolucao = transacoes.groupby('mes_ano')['valor_transacao'].sum().reset_index()
    transacoes = transacoes.merge(evolucao, on='mes_ano', how='left', suffixes=('', '_evolucao'))
    
    return transacoes

def transformar_propostas(propostas: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para propostas de crédito"""
    # Taxa de aprovação
    taxa_aprovacao = propostas.groupby('cod_colaborador')['status_proposta'].apply(
        lambda x: (x == 'Aprovada').mean() * 100).reset_index(name='taxa_aprovacao')
    
    propostas = propostas.merge(taxa_aprovacao, on='cod_colaborador', how='left')
    propostas['taxa_aprovacao'] = propostas.apply(
        lambda x: x['taxa_aprovacao'] if x['status_proposta'] == 'Aprovada' else 0, axis=1)
    
    # Valor médio por status
    media_status = propostas.groupby('status_proposta')['valor_proposta'].mean().reset_index(name='media_por_status')
    propostas = propostas.merge(media_status, on='status_proposta', how='left')
    
    return propostas

def salvar_dados(tabelas: Dict[str, pd.DataFrame], output_folder: str):
    """Salva todos os DataFrames em arquivos pickle"""
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for nome, df in tabelas.items():
        caminho = output_path / f"{nome}.pkl"
        df.to_pickle(caminho)
        print(f"✅ {nome}.pkl salvo em {caminho}")

def main():
    # Configurações
    input_path = "c:/Users/guilh/OneDrive/Documentos/GitHub/desafio_reals_bet/src/dados_extraidos.pkl"
    output_folder = "c:/Users/guilh/OneDrive/Documentos/GitHub/desafio_reals_bet/src/dados_transformados"
    
    # Pipeline principal
    tabelas = carregar_dados(input_path)
    
    # Aplicar transformações
    tabelas['clientes'] = transformar_clientes(tabelas['clientes'])
    tabelas['agencias'] = transformar_agencias(
        tabelas['agencias'], tabelas['contas'], tabelas['transacoes'])
    tabelas['transacoes'] = transformar_transacoes(tabelas['transacoes'])
    tabelas['propostas_credito'] = transformar_propostas(tabelas['propostas_credito'])
    tabelas['colaboradores'] = processar_nome_completo(tabelas['colaboradores'])
    
    # Salvar resultados
    salvar_dados(tabelas, output_folder)
    
    return tabelas

# if __name__ == "__main__":
#     dados_transformados = main()

if __name__ == "__main__":
    print("\n=== TESTE DE SANIDADE ===")
    print(f"Tipo UF_MAP: {type(get_uf_map())}")
    print(f"Tipo TIPO_CONTA_MAP: {type(get_tipo_conta_map())}")
    print(f"Exemplo SP: {get_uf_map().get('SP')}")
    
    try:
        get_uf_map()['TEST'] = 'Teste'
    except TypeError:
        print("✅ Proteção contra escrita funcionando")
    else:
        print("❌ Falha na proteção")
    
    dados_transformados = main()