# ── Ferramentas necessárias ───────────────────────────────────────────────
import requests          # Faz downloads de arquivos da internet
import pandas as pd      # Manipula tabelas de dados (como um Excel no Python)
import gspread           # Escreve dados no Google Sheets
from google.oauth2.service_account import Credentials  # Usa o JSON para autenticar no Google
from datetime import datetime  # Para mostrar data/hora nos logs
import json              # Lê o JSON das credenciais
import os                # Lê as variáveis secretas do GitHub
from io import BytesIO   # Converte bytes baixados em formato de arquivo para o pandas ler

print(f"🚀 Iniciando coleta ONS - {datetime.now().strftime('%d/%m/%Y %H:%M')}")

# ── Autenticação no Google ─────────────────────────────────────────────────
# Lê o secret GOOGLE_CREDENTIALS salvo no GitHub e transforma em dicionário Python
creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])

# Cria as credenciais usando o JSON da service account
creds = Credentials.from_service_account_info(creds_json, scopes=[
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
])

# Autoriza o acesso ao Google Sheets
gc = gspread.authorize(creds)

# Abre a planilha pelo ID (o secret SHEET_ID)
sh = gc.open_by_key(os.environ['SHEET_ID'])

# ── Configurações gerais ───────────────────────────────────────────────────
# URL base correta do bucket AWS onde o ONS armazena os arquivos públicos
BASE_URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"

# Anos que queremos coletar para comparação
ANOS = [2024, 2025, 2026]

# ── Função de download de XLSX ─────────────────────────────────────────────
def baixar_xlsx(url):
    print(f"   Tentando: {url}")

    # Faz o download com timeout de 60 segundos (xlsx pode ser maior)
    r = requests.get(url, timeout=60)

    # Se o arquivo existir (status 200), lê como Excel
    if r.status_code == 200:
        return pd.read_excel(BytesIO(r.content))

    # Se o arquivo não existir (ex: mês futuro), retorna None sem travar
    print(f"   ⚠️ Não encontrado (status {r.status_code})")
    return None

# ── Função de salvar no Google Sheets ─────────────────────────────────────
def salvar_na_aba(nome_aba, df):
    # Abre a aba pelo nome (ex: carga_horaria)
    ws = sh.worksheet(nome_aba)

    # Limpa o conteúdo anterior da aba
    ws.clear()

    # Converte tudo para texto para evitar erros de tipo de dado
    df = df.astype(str)

    # Escreve os cabeçalhos + todos os dados na planilha
    ws.update([df.columns.tolist()] + df.values.tolist())
    print(f"   ✅ {nome_aba}: {len(df)} linhas salvas")

# ── 1. Curva de Carga Horária ──────────────────────────────────────────────
# Padrão real do ONS: CURVA_CARGA_2026.xlsx (um arquivo por ano inteiro)
print("\n📊 Coletando Curva de Carga Horária...")
frames_carga = []

for ano in ANOS:
    url = f"{BASE_URL}/curva-carga-ho/CURVA_CARGA_{ano}.xlsx"
    df = baixar_xlsx(url)
    if df is not None:
        frames_carga.append(df)
        print(f"   ✔ {ano} - {len(df)} registros")

# Junta os 3 anos numa única tabela e salva na aba
if frames_carga:
    df_carga = pd.concat(frames_carga, ignore_index=True)
    salvar_na_aba("carga_horaria", df_carga)
else:
    print("   ⚠️ Nenhum dado encontrado para carga horária")

# ── 2. Fator de Capacidade Eólica e Solar ─────────────────────────────────
# Padrão real do ONS: FATOR_CAPACIDADE-2_2026_03.xlsx (um arquivo por mês/ano)
print("\n🌬️ Coletando Fator de Capacidade Eólica e Solar...")
frames_fc = []

for ano in ANOS:
    for mes in range(1, 13):
        url = f"{BASE_URL}/fator_capacidade_2_di/FATOR_CAPACIDADE-2_{ano}_{mes:02d}.xlsx"
        df = baixar_xlsx(url)
        if df is not None:
            frames_fc.append(df)
            print(f"   ✔ {ano}/{mes:02d} - {len(df)} registros")

# Junta todos os meses/anos numa única tabela e salva na aba
if frames_fc:
    df_fc = pd.concat(frames_fc, ignore_index=True)
    salvar_na_aba("fator_capacidade", df_fc)
else:
    print("   ⚠️ Nenhum dado encontrado para fator de capacidade")

# ── 3. Capacidade de Geração ───────────────────────────────────────────────
# Padrão real do ONS: CAPACIDADE_GERACAO.xlsx (arquivo único, sem ano)
print("\n⚡ Coletando Capacidade de Geração...")

url = f"{BASE_URL}/capacidade-geracao/CAPACIDADE_GERACAO.xlsx"
df_ci = baixar_xlsx(url)

if df_ci is not None:
    salvar_na_aba("capacidade_instalada", df_ci)
else:
    print("   ⚠️ Nenhum dado encontrado para capacidade de geração")

print(f"\n🎉 Coleta finalizada com sucesso! - {datetime.now().strftime('%d/%m/%Y %H:%M')}")
