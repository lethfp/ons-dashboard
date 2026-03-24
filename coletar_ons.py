# ── Ferramentas necessárias ───────────────────────────────────────────────
import requests          # Faz downloads de arquivos da internet
import pandas as pd      # Manipula tabelas de dados (como um Excel no Python)
import gspread           # Escreve dados no Google Sheets
from google.oauth2.service_account import Credentials  # Usa o JSON para autenticar no Google
from datetime import datetime  # Para mostrar data/hora nos logs
import json              # Lê o JSON das credenciais
import os                # Lê as variáveis secretas do GitHub
from io import BytesIO   # Converte bytes baixados em formato de arquivo para o pandas ler
import math              # Para verificar NaN e infinitos

print(f"🚀 Iniciando coleta ONS - {datetime.now().strftime('%d/%m/%Y %H:%M')}")

# ── Autenticação no Google ─────────────────────────────────────────────────
creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
creds = Credentials.from_service_account_info(creds_json, scopes=[
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
])
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ['SHEET_ID'])

# ── Configurações gerais ───────────────────────────────────────────────────
BASE_URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"
ANOS = [2024, 2025, 2026]

# Controle de resultados — mostra resumo no final
resultados = {}

# ── Função de download de XLSX ─────────────────────────────────────────────
def baixar_xlsx(url):
    print(f"   Tentando: {url}")
    r = requests.get(url, timeout=60)
    if r.status_code == 200:
        return pd.read_excel(BytesIO(r.content))
    print(f"   ⚠️ Não encontrado (status {r.status_code})")
    return None

# ── Função de salvar no Google Sheets ─────────────────────────────────────
def salvar_na_aba(nome_aba, df):
    ws = sh.worksheet(nome_aba)
    ws.clear()

    # Converte colunas de data para string antes de qualquer coisa
    # (evita NaT que o Google Sheets não aceita)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%d/%m/%Y %H:%M').fillna("")

    # Tratamento agressivo de valores inválidos — célula por célula
    for col in df.columns:
        df[col] = df[col].apply(lambda x:
            "" if (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))
            else x
        )

    # Limpa valores inválidos mas mantém como número
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
       ws.update([df.columns.tolist()] + df.fillna("").values.tolist())
        print(f"   ✅ {nome_aba}: {len(df)} linhas salvas")

# ── Função principal — roda cada dataset de forma independente ─────────────
def coletar(nome, funcao):
    print(f"\n{'='*60}")
    try:
        funcao()
        resultados[nome] = "✅ Sucesso"
    except Exception as e:
        print(f"   ❌ Erro em {nome}: {e}")
        resultados[nome] = f"❌ Erro: {e}"

# ── 1. Curva de Carga Horária ──────────────────────────────────────────────
def coletar_curva_carga():
    print("📊 Coletando Curva de Carga Horária...")
    frames = []
    for ano in ANOS:
        url = f"{BASE_URL}/curva-carga-ho/CURVA_CARGA_{ano}.xlsx"
        df = baixar_xlsx(url)
        if df is not None:
            frames.append(df)
            print(f"   ✔ {ano} - {len(df)} registros")
    if frames:
        salvar_na_aba("CURVA_CARGA", pd.concat(frames, ignore_index=True))
    else:
        print("   ⚠️ Nenhum dado encontrado")

# ── 2. Fator de Capacidade Eólica e Solar ─────────────────────────────────
# Estratégia: todos os meses disponíveis de 2026
def coletar_fator_capacidade():
    print("🌬️ Coletando Fator de Capacidade Eólica e Solar (mês mais recente 2026)...")

    # Tenta do mês atual para trás até achar o primeiro disponível
    mes_atual = datetime.now().month
    for mes in range(mes_atual, 0, -1):
        url = f"{BASE_URL}/fator_capacidade_2_di/FATOR_CAPACIDADE-2_2026_{mes:02d}.xlsx"
        df = baixar_xlsx(url)
        if df is not None:
            print(f"   ✔ 2026/{mes:02d} - {len(df)} registros (mais recente)")
            salvar_na_aba("FATOR_CAPACIDADE", df)
            return
    print("   ⚠️ Nenhum dado encontrado")

# ── 3. Capacidade de Geração ───────────────────────────────────────────────
def coletar_capacidade_geracao():
    print("⚡ Coletando Capacidade de Geração...")
    url = f"{BASE_URL}/capacidade-geracao/CAPACIDADE_GERACAO.xlsx"
    df = baixar_xlsx(url)
    if df is not None:
        salvar_na_aba("CAPACIDADE_INSTALADA", df)

        # Agrupa por usina somando a potência de cada unidade geradora
        # (val_potenciaefetiva é por unidade, precisamos do total da usina)
        colunas = ["id_subsistema", "id_estado", "nom_tipousina", "nom_usina", "val_potenciaefetiva"]
        df_grupo = df[colunas].copy()
        df_grupo["val_potenciaefetiva"] = pd.to_numeric(df_grupo["val_potenciaefetiva"], errors="coerce")

        # Adiciona coluna de região geográfica baseada no estado (não no subsistema elétrico do ONS)
        mapa_regiao = {
            "AC": "Norte",       "AP": "Norte",       "AM": "Norte",
            "PA": "Norte",       "RO": "Norte",       "RR": "Norte",
            "TO": "Norte",       "AL": "Nordeste",    "BA": "Nordeste",
            "CE": "Nordeste",    "MA": "Nordeste",    "PB": "Nordeste",
            "PE": "Nordeste",    "PI": "Nordeste",    "RN": "Nordeste",
            "SE": "Nordeste",    "DF": "Centro-Oeste","GO": "Centro-Oeste",
            "MT": "Centro-Oeste","MS": "Centro-Oeste","ES": "Sudeste",
            "MG": "Sudeste",     "RJ": "Sudeste",     "SP": "Sudeste",
            "PR": "Sul",         "RS": "Sul",          "SC": "Sul"
        }
        df_grupo["nom_regiao"] = df_grupo["id_estado"].map(mapa_regiao).fillna("Outros")

        # Adiciona nome completo do estado com país para o Looker Studio reconhecer corretamente
        mapa_estado = {
            "AC": "Acre, Brazil",                "AL": "Alagoas, Brazil",
            "AP": "Amapá, Brazil",              "AM": "Amazonas, Brazil",
            "BA": "Bahia, Brazil",              "CE": "Ceará, Brazil",
            "DF": "Distrito Federal, Brazil",   "ES": "Espírito Santo, Brazil",
            "GO": "Goiás, Brazil",              "MA": "Maranhão, Brazil",
            "MT": "Mato Grosso, Brazil",        "MS": "Mato Grosso do Sul, Brazil",
            "MG": "Minas Gerais, Brazil",       "PA": "Pará, Brazil",
            "PB": "Paraíba, Brazil",            "PR": "Paraná, Brazil",
            "PE": "Pernambuco, Brazil",         "PI": "Piauí, Brazil",
            "RJ": "Rio de Janeiro, Brazil",     "RN": "Rio Grande do Norte, Brazil",
            "RS": "Rio Grande do Sul, Brazil",  "RO": "Rondônia, Brazil",
            "RR": "Roraima, Brazil",            "SC": "Santa Catarina, Brazil",
            "SP": "São Paulo, Brazil",          "SE": "Sergipe, Brazil",
            "TO": "Tocantins, Brazil"
        }
        df_grupo["nom_estado"] = df_grupo["id_estado"].map(mapa_estado).fillna(df_grupo["id_estado"])

        df_agrupado = (
            df_grupo
            .groupby(["id_subsistema", "nom_regiao", "id_estado", "nom_estado", "nom_tipousina", "nom_usina"], as_index=False)
            .agg(val_potenciaefetiva_total_MW=("val_potenciaefetiva", "sum"))
            .sort_values("val_potenciaefetiva_total_MW", ascending=False)
        )
        # Arredonda para 2 casas decimais
        df_agrupado["val_potenciaefetiva_total_MW"] = df_agrupado["val_potenciaefetiva_total_MW"].round(2)
        salvar_na_aba("CAPACIDADE_AGRUPADA", df_agrupado)
        print(f"   ✅ CAPACIDADE_AGRUPADA: {len(df_agrupado)} usinas agrupadas")
    else:
        print("   ⚠️ Nenhum dado encontrado")

# ── 4. Carga de Energia Mensal ─────────────────────────────────────────────
def coletar_carga_mensal():
    print("📅 Coletando Carga de Energia Mensal...")
    url = f"{BASE_URL}/carga_energia_me/CARGA_MENSAL.xlsx"
    df = baixar_xlsx(url)
    if df is not None:
        salvar_na_aba("CARGA_ENERGIA_MENSAL", df)
    else:
        print("   ⚠️ Nenhum dado encontrado")

# ── 5. Carga de Energia Diária ─────────────────────────────────────────────
def coletar_carga_diaria():
    print("📆 Coletando Carga de Energia Diária...")
    frames = []
    for ano in ANOS:
        url = f"{BASE_URL}/carga_energia_di/CARGA_ENERGIA_{ano}.xlsx"
        df = baixar_xlsx(url)
        if df is not None:
            frames.append(df)
            print(f"   ✔ {ano} - {len(df)} registros")
    if frames:
        salvar_na_aba("CARGA_ENERGIA_DIARIA", pd.concat(frames, ignore_index=True))
    else:
        print("   ⚠️ Nenhum dado encontrado")

# ── Execução independente de cada dataset ─────────────────────────────────
coletar("Curva de Carga Horária",  coletar_curva_carga)
coletar("Fator de Capacidade",     coletar_fator_capacidade)
coletar("Capacidade de Geração",   coletar_capacidade_geracao)
coletar("Carga de Energia Mensal", coletar_carga_mensal)
coletar("Carga de Energia Diária", coletar_carga_diaria)

# ── Resumo final ───────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"📋 RESUMO - {datetime.now().strftime('%d/%m/%Y %H:%M')}")
for nome, status in resultados.items():
    print(f"   {status} — {nome}")
print('='*60)
