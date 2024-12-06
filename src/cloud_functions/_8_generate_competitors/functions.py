# ---------------------------- IMPORTS DA BIBLIOTECA PADRÃO ----------------------------

import hashlib
import http.client
import json
import logging
import os
from datetime import datetime
import string
import warnings
import re  # Importado para tratar o parsing do JSON

# ---------------------------- IMPORTS DE TERCEIROS ----------------------------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from rapidfuzz import fuzz
from scipy.stats import linregress
from sklearn.linear_model import LinearRegression
import openai
from src.common.bigquery_connector import BigQueryManager
from src.common.firestore_connector import FirestoreManager
from src.config import settings

# ---------------------------- CONFIGURAÇÕES INICIAIS ----------------------------

# Suprimindo warnings desnecessários
warnings.filterwarnings("ignore")

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Defina suas chaves de API (⚠️ *Importante:* Nunca compartilhe suas chaves de API publicamente.)
SERPER_API_KEY = 'c456f4422bae73402357cd26c14e2976156ce8ba'
OPENAI_API_KEY = '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'

# Configuração da API OpenAI
openai.api_key = OPENAI_API_KEY

# Diretórios para armazenar os caches das buscas
CACHE_DIR = 'cache_shopping_searches'
OPENAI_CACHE_DIR = 'cache_openai_rankings'  # Novo diretório de cache para OpenAI

# Criar os diretórios de cache se não existirem
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OPENAI_CACHE_DIR, exist_ok=True)  # Criar diretório de cache para OpenAI

# Inicializar o cliente BigQuery
bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)
firestore_manager = FirestoreManager(credentials_path=settings.PATH_SERVICE_ACCOUNT, project_id='datalake-meli-dev')

# ---------------------------- FUNÇÕES UTILITÁRIAS ----------------------------

def gerar_hash(params):
    """
    Gera um hash SHA-256 único baseado nos parâmetros de busca e na data atual.

    Parâmetros:
    - params (dict): Parâmetros de busca.

    Retorna:
    - str: Hash gerado.
    """
    # Incluir a data atual nos parâmetros para que o cache seja diário
    params_com_data = params.copy()
    params_com_data['date'] = datetime.now().strftime('%Y-%m-%d')
    params_string = json.dumps(params_com_data, sort_keys=True)
    return hashlib.sha256(params_string.encode('utf-8')).hexdigest()

def carregar_cache(hash_key, firestore_manager=firestore_manager, collection_name='query_cache'):
    """
    Carrega dados do cache a partir do Firestore, caso exista.
    """
    doc_ref = firestore_manager.client.collection(collection_name).document(hash_key)
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict().get('data', None)
    return None

def salvar_cache(hash_key, data, firestore_manager=firestore_manager, collection_name='query_cache'):
    """
    Salva os dados no cache.

    Parâmetros:
    - hash_key (str): Chave hash para identificar o cache.
    - data (dict): Dados a serem salvos.
    - cache_dir (str): Diretório de cache.
    """
    doc_ref = firestore_manager.client.collection(collection_name).document(hash_key)
    doc_ref.set({'data': data})

def chamar_api_serper(params, api_key):
    """
    Chama a API do Serper para obter resultados do Google Shopping.

    Parâmetros:
    - params (dict): Parâmetros de busca.
    - api_key (str): Chave da API Serper.

    Retorna:
    - dict: Resposta da API.
    """
    if not api_key:
        raise ValueError("API key para Serper não está definida.")

    conn = http.client.HTTPSConnection("google.serper.dev")
    payload = json.dumps(params)
    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }
    try:
        conn.request("POST", "/shopping", payload, headers)
        res = conn.getresponse()
        data = res.read()
        logging.info(f"Chamada à API Serper - Status: {res.status}, Reason: {res.reason}")
        if res.status != 200:
            raise Exception(f"Erro na chamada da API: {res.status} {res.reason}. Mensagem: {data.decode('utf-8')}")
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        raise Exception(f"Erro ao chamar a API do Serper: {e}")

def processar_preco(preco_str):
    """
    Processa a string de preço para extrair o valor numérico.

    Parâmetros:
    - preco_str (str): String contendo o preço.

    Retorna:
    - float ou None: Valor numérico do preço ou None se não puder ser processado.
    """
    preco_str = preco_str.lower().replace('r$', '').replace('.', '').replace(',', '.').strip()
    try:
        if '/mês' in preco_str:
            return float(preco_str.replace('/mês', '').strip()) * 12
        elif '+' in preco_str:
            return float(preco_str.split('+')[0].strip())
        elif ' usado' in preco_str:
            return float(preco_str.replace(' usado', '').strip())
        else:
            return float(preco_str)
    except ValueError:
        return None

def extrair_dominio(source):
    """
    Extrai o domínio de origem do produto.

    Parâmetros:
    - source (str): Fonte do produto.

    Retorna:
    - str: Domínio extraído.
    """
    return source.lower()


# ---------------------------- FUNÇÕES PRINCIPAIS ----------------------------

def obter_ranking_com_cache(ranking_params):
    """
    Verifica o cache para a solicitação de ranking. Se não estiver no cache, chama a API OpenAI,
    salva a resposta no cache e retorna o ranking.

    Parâmetros:
    - ranking_params (dict): Parâmetros para a solicitação de ranking.

    Retorna:
    - list ou None: Ranking dos produtos ou None em caso de falha.
    """
    # Gerar hash único para os parâmetros de ranking
    hash_key = hashlib.sha256(json.dumps(ranking_params, sort_keys=True).encode('utf-8')).hexdigest()

    # Tentar carregar o ranking do cache OpenAI
    cached_ranking = carregar_cache(hash_key)
    if cached_ranking:
        logging.info("Ranking carregado do cache OpenAI.")
        return cached_ranking

    logging.info("Ranking não encontrado no cache OpenAI. Chamando a API para gerar ranking.")

    # Se não estiver no cache, proceder com a chamada à API OpenAI
    produtos_com_precos = [
        f"{i+1}. {title} - Preço: R$ {price:.2f}"
        for i, (title, price) in enumerate(zip(ranking_params['product_titles'], ranking_params['product_prices']))
    ]

    # Criar o prompt com instruções detalhadas e exemplos
    prompt = f"""
    Você é um assistente especializado em comparar produtos com base na similaridade de descrições e preços em relação a um produto alvo. Seu objetivo é analisar a lista de produtos fornecida e classificá-los em ordem de similaridade com o produto alvo, considerando tanto as características técnicas quanto o preço.

    **Produto Alvo:**
    Título: "{ranking_params['target_title']}"
    Preço: R$ {ranking_params['target_price']:.2f}

    **Lista de Produtos a Serem Avaliados:**
    {chr(10).join(produtos_com_precos)}

    **Instruções de Classificação:**
    Avalie cada produto na lista e atribua uma posição no ranking de similaridade ao produto alvo, seguindo os critérios abaixo:

    1. **Similaridade de Descrição:**
       - Produtos com descrições muito parecidas com o produto alvo devem ser classificados mais altos. Isso inclui correspondência em atributos como marca, modelo, especificações técnicas, funcionalidades, cor, tamanho e outros detalhes relevantes.
       - Produtos que pertencem à mesma categoria, mas diferem em características importantes, devem ser classificados mais abaixo.

    2. **Preço:**
       - Produtos com preços dentro de ±30% do preço do produto alvo são considerados mais similares em termos econômicos. Preços fora desse intervalo devem reduzir a pontuação de similaridade.
       - Considere o valor agregado pelo preço. Por exemplo, se um produto é significativamente mais barato, mas oferece funcionalidades similares, pode ser considerado um substituto econômico.

    3. **Marcas e Qualidade:**
       - Leve em conta a reputação da marca. Marcas conhecidas por qualidade superior podem não ser substitutas diretas de marcas genéricas, mesmo que as especificações sejam semelhantes.
       - Produtos de marcas premium ou de luxo devem ser avaliados com cautela, pois podem atender a públicos diferentes.

    4. **Substituibilidade Econômica:**
       - Considere se o produto pode ser um substituto viável em termos de uso e funcionalidade, mesmo que não seja idêntico.
       - Produtos que atendem ao mesmo propósito ou necessidade, dentro da mesma faixa de preço, devem ser considerados mais similares.

    **Exemplos para Contextualização:**

    - **Exemplo 1: Dispositivos de Automação Residencial**
      - **Produto Alvo:** "Interruptor Wi-Fi Inteligente de 3 Botões Compatível com Alexa e Google Home"
      - **Produto A:** "Interruptor Wi-Fi Inteligente de 2 Botões Compatível com Alexa e Google Home"
        - **Análise:** Similar, mas com menos botões; classificado abaixo do produto alvo.
      - **Produto B:** "Interruptor Wi-Fi Inteligente de 3 Botões com Sensor de Movimento"
        - **Análise:** Similar, com funcionalidade adicional; considerar se a diferença é relevante para o usuário.

    - **Exemplo 2: Equipamentos de Ciclismo**
      - **Produto Alvo:** "Par de Pneus MTB Chaoyang Phantom Wet Aro 29x2.20"
      - **Produto A:** "Par de Pneus MTB Chaoyang Phantom Dry Aro 29x2.20"
        - **Análise:** Similar, mas projetado para condições secas; classificar um pouco abaixo.
      - **Produto B:** "Par de Pneus MTB de outra marca com mesmas especificações"
        - **Análise:** Similar em especificações, diferença na marca; avaliar a reputação da marca.

    - **Exemplo 3: Acessórios para Armas de Fogo**
      - **Produto Alvo:** "Coldre Velado para Glock G17/G19 Gen4/Gen5"
      - **Produto A:** "Coldre Velado Universal para Pistolas Compactas"
        - **Análise:** Pode ser usado com o mesmo modelo, mas não é específico; classificar abaixo.
      - **Produto B:** "Coldre Externo para Glock G17"
        - **Análise:** Diferente no estilo de uso (externo vs. velado); menos similar.

    - **Exemplo 4: Materiais de Construção**
      - **Produto Alvo:** "Cantoneira Reforço Canto PVC com Fibra de Vidro - 1,25m"
      - **Produto A:** "Cantoneira de PVC Simples - 1,25m"
        - **Análise:** Similar, mas sem reforço de fibra de vidro; potencialmente menos durável.
      - **Produto B:** "Cantoneira de Alumínio para Acabamento"
        - **Análise:** Material diferente; pode não ser substituto direto.

    - **Exemplo 5: Equipamentos de Pesca**
      - **Produto Alvo:** "Caiaque Truta Caiaker para Pesca e Lazer"
      - **Produto A:** "Caiaque de Pesca Caiaker Lambari"
        - **Análise:** Similar em uso, modelo diferente; avaliar especificações.
      - **Produto B:** "Barco Inflável para Pesca com Motor"
        - **Análise:** Tipo de embarcação diferente; menos similar.

    **Formato da Resposta:**
    Retorne a resposta **exclusivamente no formato JSON** com o seguinte layout:


    json
        [
            {{"produto": "Título do produto 1", "preco": Preço, "rank": Posição no ranking}},
            {{"produto": "Título do produto 2", "preco": Preço, "rank": Posição no ranking}},
            ...
        ]

    **Observações Importantes:**

    - Não inclua explicações ou textos fora do JSON.
    - Seja consistente no formato e certifique-se de incluir todos os produtos fornecidos na lista.
    - O ranking deve começar em 1 para o produto mais similar.
    - Se dois produtos forem igualmente similares, desempate pelo preço mais próximo ao do produto alvo.
    - Garanta que o JSON esteja completo e corretamente formatado.
    - Analise todos os produtos mas só precisa fazer o ranking no json até a posicao 20 (vamos colocar os 20 mais parecidos o resto descarte).
    - se tiver produtos identicos nao precisa chamar de concorrente (mesma url e mesmo preço sao produtos identicos mesmo que a descricao seja diferente)
    """

    try:
        logging.info("Enviando solicitação à API OpenAI para rankeamento de produtos.")

        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Você é um assistente útil que classifica produtos com base na similaridade e preço."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2048,
            temperature=0
        )

        ranking_json = response.choices[0].message.content.strip()
        # logging.debug(f"Resposta da OpenAI: {ranking_json}")

        # Remover formatação de backticks, se houver
        if ranking_json.startswith("```json"):
            ranking_json = ranking_json[7:]  # Remover ```json
        if ranking_json.endswith("```"):
            ranking_json = ranking_json[:-3]  # Remover ```

        # Tentar carregar o JSON
        try:
            ranking_data = json.loads(ranking_json)
        except json.JSONDecodeError:
            logging.error("Erro ao decodificar o JSON retornado pela API.")
            logging.error(f"Resposta bruta: {ranking_json}")
            return None

        # Verificar consistência do ranking
        if len(ranking_data) != len(ranking_params['product_titles']):
            logging.warning("O número de itens no JSON não corresponde ao número de produtos enviados.")
            return None

        # Salvar o ranking no cache OpenAI
        salvar_cache(hash_key, ranking_data)
        logging.info("Ranking salvo no cache OpenAI.")

        return ranking_data
    except Exception as e:
        logging.error(f"Erro ao chamar a API da OpenAI: {e}")
        return None

def rankear_produtos(target_title, target_price, product_titles, product_prices, max_retries=3):
    """
    Wrapper para obter ranking com cache e tratamento de resposta.
    """
    ranking_params = {
        "target_title": target_title,
        "target_price": target_price,
        "product_titles": product_titles,
        "product_prices": product_prices,
        "date": datetime.now().strftime('%Y-%m-%d')
    }
    return obter_ranking_com_cache(ranking_params)

def get_competitors_for_top_n(selected_items_df, top_n=10):
    """
    Seleciona os top_n itens por seller_id com maior quantidade vendida e busca seus concorrentes.
    Retorna um DataFrame com os dados dos top_n itens e seus concorrentes, incluindo o ranking de similaridade.

    Parâmetros:
    - selected_items_df (DataFrame): DataFrame com os itens selecionados (incluindo 'group_id', 'total_sold_quantity')
    - top_n (int): Número de top itens por seller_id a serem processados (default=10)

    Retorna:
    - DataFrame: DataFrame com os top_n itens e seus concorrentes
    """
    # Selecionar os top_n itens por seller_id excluindo o seller_id especificado
    try:
        logging.info(f"Selecionando os top {top_n} itens por seller_id com maior quantidade vendida.")
        top_n_df = selected_items_df[selected_items_df['seller_id'] != 189643563].groupby('seller_id').apply(
            lambda x: x.nlargest(top_n, 'total_sold_quantity')
        ).reset_index(drop=True)
        logging.info(f"Selecionados {len(top_n_df)} itens para processamento de concorrentes.")
    except Exception as e:
        logging.error(f"Erro ao selecionar top_n itens por seller_id: {e}")
        return pd.DataFrame()

    # Lista para armazenar os dados dos concorrentes
    concorrentes_lista = []

    # Iterar sobre cada item no top_n_df
    for idx, row in top_n_df.iterrows():
        item_group_id = row['group_id']
        item_id = row['item_id']
        item_name = row['item_name']
        seller_id = row['seller_id']
        price = row['price']
        total_sold_quantity = row['total_sold_quantity']

        logging.info(f"Processando concorrentes para o item: {item_name} (Item ID: {item_id}, Group ID: {item_group_id}, Seller ID: {seller_id})")

        # Definir os parâmetros da consulta para a API do Serper
        search_query = item_name
        search_params = {
            "q": search_query,
            "location": "Brazil",
            "gl": "br",
            "hl": "pt-br",
            "num": 50,
            "page": 1
        }

        # Gerar um hash único para os parâmetros de busca
        hash_key = gerar_hash(search_params)

        # Tentar carregar os dados do cache
        cached_response = carregar_cache(hash_key)

        if cached_response:
            logging.info("Dados carregados do cache.")
            response = cached_response
        else:
            logging.info("Realizando busca na API do Serper...")
            try:
                response = chamar_api_serper(search_params, SERPER_API_KEY)
                salvar_cache(hash_key, response)
                logging.info("Dados salvos no cache.")
            except Exception as e:
                logging.error(f"Erro ao obter dados da API do Serper: {e}")
                continue  # Pular para o próximo item

        # Verificar se há resultados na resposta
        if 'shopping' not in response:
            logging.warning("Nenhum resultado de shopping encontrado para este item.")
            continue  # Pular para o próximo item

        # Criar o DataFrame a partir dos resultados da API
        try:
            concorrente_df = pd.json_normalize(response['shopping'])
            logging.info(f"{len(concorrente_df)} concorrentes encontrados para o item: {item_name}")
        except Exception as e:
            logging.error(f"Erro ao normalizar os dados para DataFrame: {e}")
            continue  # Pular para o próximo item

        # Processar os dados dos concorrentes
        concorrente_df['competitor_group_id'] = item_group_id
        concorrente_df['competitor_item_name'] = concorrente_df['title']
        concorrente_df['competitor_price'] = concorrente_df['price'].apply(processar_preco)
        concorrente_df['competitor_url'] = concorrente_df['link']
        concorrente_df['competitor_domain'] = concorrente_df['source'].apply(extrair_dominio)

        # Adicionar a coluna de data
        current_date = datetime.now().strftime('%Y-%m-%d')
        concorrente_df['data'] = current_date

        # Adicionar o ranking ao DataFrame de concorrentes
        # Preparar os dados para o ranking incluindo todas as colunas
        ranking = rankear_produtos(
            target_title=item_name,
            target_price=price,
            product_titles=concorrente_df['competitor_item_name'].tolist(),
            product_prices=concorrente_df['competitor_price'].tolist()
        )

        if ranking is None:
            logging.warning("Falha ao obter ranking dos concorrentes. Pulando este item.")
            continue  # Pular para o próximo item

        concorrente_df['rank'] = [item['rank'] for item in ranking]

        # Adicionar informações do top_n_df ao concorrente_df
        concorrente_df['top_item_id'] = item_id
        concorrente_df['top_item_name'] = item_name
        concorrente_df['top_seller_id'] = seller_id
        concorrente_df['top_price'] = price
        concorrente_df['top_total_sold_quantity'] = total_sold_quantity

        # Rearranjar as colunas para incluir todas as informações
        # Manter todas as colunas originais da API SERPER e adicionar as novas colunas
        # Não remover nenhuma coluna existente
        concorrente_final = concorrente_df.copy()

        # Adicionar ao lista de concorrentes
        concorrentes_lista.append(concorrente_final)

    if not concorrentes_lista:
        logging.warning("Nenhum concorrente encontrado para os itens selecionados.")
        return pd.DataFrame()

    # Concatenar todos os DataFrames de concorrentes
    concorrentes_df = pd.concat(concorrentes_lista, ignore_index=True)
    logging.info(f"Total de concorrentes coletados: {len(concorrentes_df)}")

    return concorrentes_df

# ---------------------------- PROCESSAMENTO DE DADOS ----------------------------

def load_and_process_sales_data():
    """
    Carrega os dados de vendas do BigQuery, pré-processa os nomes dos produtos,
    agrupa produtos com nomes iguais e similares, e seleciona os itens com maior venda por group_id e seller_id.

    Retorna:
    - DataFrame: DataFrame com os itens selecionados.
    """
    # 1.1 Load Sales Data
    query = """
    SELECT
        item_id,
        item_name, 
        seller_id, 
        url,
        AVG(price) AS price,
        SUM(stock) AS stock,
        SUM(sold_quantity) AS sold_quantity
    FROM datalake-v2-424516.datalake_v2.items_details 
    WHERE correspondent_date BETWEEN DATETIME("2024-11-30") AND DATETIME_ADD(DATETIME("2024-11-30"), INTERVAL 1 DAY) 
        AND fg_mshops = true AND status = 'active'
    GROUP BY 1, 2, 3, 4
    """

    try:
        logging.info("Executando consulta no BigQuery.")
        df_raw = bigquery.run_query(query)
        df = df_raw.copy(deep=True)
        logging.info(f"Dados de vendas carregados com sucesso. Número de anúncios: {len(df)}")
    except Exception as e:
        logging.error(f"Erro ao carregar dados do BigQuery: {e}")
        df = pd.DataFrame()

    if df.empty:
        logging.error("O DataFrame de vendas está vazio. Verifique a consulta e os dados no BigQuery.")
        raise ValueError("O DataFrame de vendas está vazio. Verifique a consulta e os dados no BigQuery.")

    # Função para pré-processar os nomes dos produtos
    def preprocess_name(name):
        """
        Pré-processa o nome do produto:
        - Converte para minúsculas
        - Remove pontuação
        - Remove espaços extras

        Parâmetros:
        - name (str): Nome do produto.

        Retorna:
        - str: Nome do produto pré-processado.
        """
        # Converter para minúsculas
        name = name.lower()
        # Remover pontuação
        translator = str.maketrans('', '', string.punctuation)
        name = name.translate(translator)
        # Remover espaços extras
        name = ' '.join(name.split())
        return name

    df['processed_name'] = df['item_name'].apply(preprocess_name)
    logging.info("Pré-processamento dos nomes dos produtos concluído.")

    # Função personalizada para agregação
    def aggregate_group(group):
        """
        Agrega os dados de cada grupo.

        Parâmetros:
        - group (DataFrame): Grupo de produtos similares.

        Retorna:
        - Series: Dados agregados do grupo.
        """
        item_ids = group['item_id'].tolist()
        urls = group['url'].tolist()
        total_stock = group['stock'].sum()
        total_sold_quantity = group['sold_quantity'].sum()

        # Verificar se a soma dos sold_quantity é maior que zero
        if total_sold_quantity > 0:
            weighted_avg_price = np.average(group['price'], weights=group['sold_quantity'])
        else:
            weighted_avg_price = np.nan  # ou outro valor padrão, se preferir

        return pd.Series({
            'item_ids': item_ids,
            'urls': urls,
            'total_stock': total_stock,
            'total_sold_quantity': total_sold_quantity,
            'weighted_avg_price': weighted_avg_price
        })

    # Agrupando por 'processed_name' e aplicando a função personalizada
    try:
        logging.info("Agrupando dados por nome processado.")
        grouped = df.groupby('processed_name').apply(aggregate_group).reset_index()
        logging.info(f"Agrupamento concluído. Número de grupos com nomes iguais: {len(grouped)}")
    except Exception as e:
        logging.error(f"Erro ao agrupar dados: {e}")
        grouped = pd.DataFrame()

    if grouped.empty:
        logging.error("O DataFrame agrupado está vazio. Verifique os dados de entrada.")
        raise ValueError("O DataFrame agrupado está vazio. Verifique a consulta e os dados no BigQuery.")

    # Função para agrupar similaridade baseada em nome e preço
    def group_similar_names_with_price(df, name_col='processed_name', price_col='weighted_avg_price', name_threshold=80, price_threshold=0.15):
        """
        Agrupa produtos similares com base na similaridade de nomes e na diferença de preços.

        Parâmetros:
        - df (DataFrame): DataFrame agrupado.
        - name_col (str): Nome da coluna com os nomes processados.
        - price_col (str): Nome da coluna com os preços.
        - name_threshold (int): Limite de similaridade de nomes (0-100).
        - price_threshold (float): Limite de diferença relativa de preço (e.g., 0.15 para 15%).

        Retorna:
        - list: Lista de grupos similares.
        """
        groups = []
        group_ids = {}
        current_group_id = 0

        # Ordenar o dataframe por preço para facilitar o agrupamento
        df_sorted = df.sort_values(by=price_col).reset_index(drop=True)

        for idx, row in df_sorted.iterrows():
            name = row[name_col]
            price = row[price_col]

            if name in group_ids:
                continue

            # Inicializar um novo grupo
            groups.append({
                'group_id': current_group_id,
                'item_names': [row['processed_name']],
                'original_names': [row['processed_name']],  # Armazenar nomes originais se necessário
                'prices': [price]
            })
            group_ids[name] = current_group_id

            # Comparar com os demais itens
            for jdx in range(idx + 1, len(df_sorted)):
                other_name = df_sorted.at[jdx, name_col]
                other_price = df_sorted.at[jdx, price_col]

                if other_name in group_ids:
                    continue

                # Calcular similaridade de nome
                name_similarity = fuzz.token_set_ratio(name, other_name)

                # Calcular diferença relativa de preço
                if pd.isna(price) or pd.isna(other_price):
                    price_similarity = False
                else:
                    relative_diff = abs(price - other_price) / price
                    price_similarity = relative_diff <= price_threshold  # Ex: 15% de diferença

                # Verificar se ambos os critérios são atendidos
                if name_similarity >= name_threshold and price_similarity:
                    groups[current_group_id]['item_names'].append(other_name)
                    groups[current_group_id]['prices'].append(other_price)
                    group_ids[other_name] = current_group_id

            current_group_id += 1

        return groups

    # Aplicar a função de agrupamento com critérios ajustados
    try:
        logging.info("Agrupando produtos similares com base em nome e preço.")
        similar_groups = group_similar_names_with_price(
            grouped,
            name_col='processed_name',
            price_col='weighted_avg_price',
            name_threshold=80,   # Ajustado para permitir mais variação nos nomes
            price_threshold=0.15  # Ajustado para permitir até 15% de diferença de preço
        )
        logging.info(f"Agrupamento de similaridade concluído. Número de grupos similares: {len(similar_groups)}")
    except Exception as e:
        logging.error(f"Erro ao agrupar similaridade de produtos: {e}")
        similar_groups = []

    # Criar um DataFrame com os grupos
    groups_df = pd.DataFrame(similar_groups)
    logging.info("DataFrame de grupos por similaridade criado.")

    # Criar um dicionário mapeando cada processed_name para seu group_id
    name_to_group = {}
    for group in similar_groups:
        group_id = group['group_id']
        for name in group['item_names']:
            name_to_group[name] = group_id

    # Adicionar a coluna 'group_id' ao DataFrame agrupado
    grouped['group_id'] = grouped['processed_name'].map(name_to_group)
    logging.info("Atribuição de group_id concluída.")

    # 4. Verificar e Atribuir group_id para processed_names sem group_id
    nan_group_ids = grouped['group_id'].isna()
    num_nan = nan_group_ids.sum()

    if num_nan > 0:
        logging.info(f"Encontrado {num_nan} processed_name(s) sem group_id. Atribuindo group_ids únicos.")

        # Encontrar o maior group_id existente
        max_group_id = grouped['group_id'].max()
        if pd.isna(max_group_id):
            max_group_id = -1
        else:
            max_group_id = int(max_group_id)

        # Criar novos group_ids para os nomes sem group_id
        new_group_ids = range(max_group_id + 1, max_group_id + 1 + num_nan)

        # Obter os processed_names sem group_id
        nan_names = grouped.loc[nan_group_ids, 'processed_name'].tolist()

        # Atualizar o DataFrame 'grouped' com os novos group_ids
        grouped.loc[nan_group_ids, 'group_id'] = new_group_ids

        # Atualizar o dicionário de mapeamento
        for name, new_id in zip(nan_names, new_group_ids):
            name_to_group[name] = new_id

        logging.info("Atribuição de group_ids para nomes sem group_id concluída.")
    else:
        logging.info("Nenhum group_id faltante encontrado.")

    # Garantir que todos os group_ids estão atribuídos
    assert not grouped['group_id'].isna().any(), "Ainda existem group_ids faltantes após a atribuição."

    # Mapear o 'group_id' de volta para o DataFrame original
    df = df.merge(grouped[['processed_name', 'group_id', 'total_sold_quantity']], on='processed_name', how='left')

    # Verificar se todos os itens possuem group_id
    missing_group_ids = df['group_id'].isna().sum()
    if missing_group_ids > 0:
        logging.warning(f"{missing_group_ids} item(s) não possuem group_id após o mapeamento.")

    # 5. Selecionar o item_name com Maior Venda por group_id e por seller_id
    try:
        logging.info("Selecionando o item_name com maior venda por group_id e seller_id.")
        # Primeiro, selecionar o item com maior sold_quantity por group_id e seller_id
        selected_items = df.loc[df.groupby(['group_id', 'seller_id'])['sold_quantity'].idxmax()]
        # Selecionar as colunas desejadas
        selected_items_df = selected_items[['group_id', 'item_id', 'item_name', 'seller_id', 'price', 'total_sold_quantity']].reset_index(drop=True)
        logging.info(f"Seleção concluída. Número de itens selecionados: {len(selected_items_df)}")
    except Exception as e:
        logging.error(f"Erro ao selecionar itens com maior venda: {e}")
        selected_items_df = pd.DataFrame()

    # Garantir que cada (group_id, seller_id) está presente apenas uma vez
    assert selected_items_df.groupby(['group_id', 'seller_id']).size().max() == 1, "Existem (group_id, seller_id) duplicados na seleção!"

    logging.info("DataFrame selecionado com o item_name de maior venda por group_id e seller_id criado.")

    return selected_items_df
