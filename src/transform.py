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
    """Calcula idade a partir de data de nascimento no formato DD/MM/YYYY"""
    if col_data in df.columns:
        # Converte a coluna diretamente para datetime (sem criar nova coluna)
        df[col_data] = pd.to_datetime(
            df[col_data], 
            format='%d/%m/%Y',  # Força o formato brasileiro
            errors='coerce'      # Converte erros em NaT
        )
        
        # Calcular idade (dias // 365)
        df['idade'] = ((pd.to_datetime('today') - df[col_data]).dt.days // 365)

        # Converte para inteiro
        df['idade'] = df['idade'].astype('Int64')

    return df

# Chamadas para usar as funções de acesso
def transformar_clientes(clientes: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para clientes"""
    # Aplicando mapeamento do dicionário
    clientes['uf'] = clientes['endereco'].apply(extrair_uf)
    clientes = aplicar_mapeamento(clientes, 'uf', get_uf_map)
    clientes = aplicar_mapeamento(clientes, 'tipo_cliente', get_tipo_cliente_map)

    # Aplicando ajuste do nome completo
    clientes = processar_nome_completo(clientes)
    
    # Tempo como cliente desde sua data de inclusão
    clientes['data_inclusao'] = pd.to_datetime(clientes['data_inclusao'], errors='coerce').dt.tz_localize(None)
    clientes['tempo_como_cliente_meses'] = ((pd.Timestamp.now().tz_localize(None) - clientes['data_inclusao']).dt.days / 30).round().astype(int) # Por Mês
    
    # Idade
    clientes = calcular_idade(clientes, 'data_nascimento')
    
    return clientes

def transformar_agencias(agencias: pd.DataFrame, contas: pd.DataFrame, transacoes: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para agências"""
    # Aplicando mapeamento do dicionário com nome do estado
    agencias = aplicar_mapeamento(agencias, 'uf', get_uf_map)
    
    # Merge para cálculos
    contas = pd.merge(contas, agencias, on='cod_agencia', how='left')
    transacoes_contas = pd.merge(transacoes, contas, on='num_conta', how='left')
    transacoes_agencias = pd.merge(transacoes_contas, agencias, on='cod_agencia', how='left')
    
    # Cálculos agregados
    agencias['saldo_medio'] = agencias['cod_agencia'].map(
        contas.groupby('cod_agencia')['saldo_disponivel'].mean())
    
    agencias['num_contas'] = agencias['cod_agencia'].map(
        contas.groupby('cod_agencia')['num_conta'].count())
    
    agencias['volume_transacoes'] = agencias['cod_agencia'].map(
        transacoes_agencias.groupby('cod_agencia')['valor_transacao'].sum())

    return agencias

def transformar_contas(contas: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para contas"""
    # Aplicando mapeamento do dicionário com nome do tipo da conta
    contas = aplicar_mapeamento(contas, 'tipo_conta', get_tipo_conta_map)

    return contas

def transformar_transacoes(transacoes: pd.DataFrame, contas: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para transações"""
    # Extraindo a data para cálculo de evolução
    transacoes['data_transacao'] = pd.to_datetime(transacoes['data_transacao'], errors='coerce', utc=True)
    transacoes['mes_ano'] = transacoes['data_transacao'].dt.tz_localize(None).dt.to_period('M').astype(str)
    
    # --- Merge com dados das contas ---
    transacoes_com_saldo  = transacoes.merge(
        contas[['num_conta', 'saldo_disponivel']],
        on='num_conta',
        how='left'
    )
    
    # Calcula a média de transações por conta (normalizado pelo saldo disponível)
    transacoes['valor_medio_conta'] = transacoes['num_conta'].map(transacoes.groupby('num_conta')['valor_transacao'].mean())

    # Razão entre o valor da transação e o saldo disponível (evita valores absurdos)
    # Representa o impacto da transação em percentual no saldo disponível da conta
    # Valores próximos a 1.0 (ou acima) indicam risco de liquidez, pode ser sinal de má gestão ou fraude.
    transacoes['valor_vs_saldo'] = (transacoes_com_saldo['valor_transacao'].abs() / transacoes_com_saldo['saldo_disponivel'].replace(0, 1e-6))  # Evita divisão por zero

    # Frequência de transações por conta
    transacoes['freq_transacoes'] = transacoes.groupby('num_conta')['num_conta'].transform('count')
    
    # Evolução mensal de transações (Evita que transações anômalas, erros ou fraudes, distorçam a análise)
    transacoes['valor_transacao'] = transacoes['valor_transacao'].clip(
        lower=-1e6,  # Limite inferior: -1 milhão
        upper=1e6    # Limite superior: +1 milhão
    )
    transacoes['valor_transacao'] = transacoes['valor_transacao'].clip(lower=-1e6, upper=1e6)
    evolucao = transacoes.groupby('mes_ano')['valor_transacao'].sum().reset_index()    
    transacoes = transacoes.merge(evolucao, on='mes_ano', how='left', suffixes=('', '_evolucao')) # Merge com os dataframes transformados 
    
    return transacoes

def transformar_propostas(propostas_credito: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de transformação para propostas de crédito"""
    
    # Taxa de aprovação por colaborador
    taxa_colab = (
        propostas_credito.groupby('cod_colaborador')['status_proposta']
        .apply(lambda x: (x == 'Aprovada').sum())  # Soma de aprovadas
        .reset_index(name='num_aprovadas')
    )
    
    total_propostas = (
        propostas_credito.groupby('cod_colaborador').size()
        .reset_index(name='total_propostas')
    )
    
    # Merge para taxa dos colaboradres
    taxa_colab = taxa_colab.merge(total_propostas, on='cod_colaborador')
    taxa_colab['taxa_aprovacao_colab'] = (taxa_colab['num_aprovadas'] / taxa_colab['total_propostas']) * 100
    taxa_colab = taxa_colab[['cod_colaborador', 'taxa_aprovacao_colab']]

    # Taxa de aprovação por cliente
    taxa_cliente = (
        propostas_credito.groupby('cod_cliente')['status_proposta']
        .apply(lambda x: (x == 'Aprovada').sum())
        .reset_index(name='num_aprovadas')
    )

    total_clientes = (
        propostas_credito.groupby('cod_cliente').size()
        .reset_index(name='total_propostas')
    )

    # Merge para taxa dos clientes
    taxa_cliente = taxa_cliente.merge(total_clientes, on='cod_cliente')
    taxa_cliente['taxa_aprovacao_cliente'] = (taxa_cliente['num_aprovadas'] / taxa_cliente['total_propostas']) * 100
    taxa_cliente = taxa_cliente[['cod_cliente', 'taxa_aprovacao_cliente']]

    # Valor médio da proposta por status e cliente
    media_status_cliente = (propostas_credito
                          .groupby(['status_proposta', 'cod_cliente'])['valor_proposta']
                          .mean()
                          .reset_index(name='media_status_cliente'))
    
    # Contagem de propostas por cliente (frequência)
    count_cliente = (propostas_credito
                   .groupby('cod_cliente')
                   .size()
                   .reset_index(name='total_propostas_cliente'))
    
    # Merge todas as métricas
    propostas_credito = propostas_credito.merge(taxa_colab, on='cod_colaborador', how='left')
    propostas_credito = propostas_credito.merge(taxa_cliente, on='cod_cliente', how='left')
    propostas_credito = propostas_credito.merge(media_status_cliente, 
                                              on=['status_proposta', 'cod_cliente'], 
                                              how='left')
    propostas_credito = propostas_credito.merge(count_cliente, on='cod_cliente', how='left')
    
    # Diferença entre valor da proposta e média do cliente - análise de risco
    propostas_credito['diferenca_media_cliente'] = \
        propostas_credito['valor_proposta'] - propostas_credito['media_status_cliente']
    
     # Zerar taxas para propostas não aprovadas
    propostas_credito['taxa_aprovacao_colab'] = propostas_credito.apply(
        lambda x: x['taxa_aprovacao_colab'] if x['status_proposta'] == 'Aprovada' else 0,
        axis=1
    )
    
    propostas_credito['taxa_aprovacao_cliente'] = propostas_credito.apply(
        lambda x: x['taxa_aprovacao_cliente'] if x['status_proposta'] == 'Aprovada' else 0,
        axis=1
    )

    return propostas_credito

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
    print(tabelas['clientes'].head())
    tabelas['agencias'] = transformar_agencias(tabelas['agencias'], tabelas['contas'], tabelas['transacoes'])
    tabelas['contas'] = transformar_contas(tabelas['contas'])
    tabelas['transacoes'] = transformar_transacoes(tabelas['transacoes'], tabelas['contas'])
    # print(tabelas['transacoes'].head())
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