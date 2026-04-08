import os
import re
from io import BytesIO
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
import psycopg2
from fpdf import FPDF
from unidecode import unidecode
import sys
import webbrowser
from threading import Timer
from dotenv import load_dotenv

load_dotenv()

# ================================================================
# CONFIGURAÇÕES
# ================================================================
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.abspath(".")

DATABASE_URL = os.getenv("DATABASE_URL")

app = Flask(__name__)
app.secret_key = "chave_mestra_123"

DF_MATCH = None


# ================================================================
# CONEXÃO COM NEON
# ================================================================
def get_conn():
    return psycopg2.connect(DATABASE_URL)


def get_base_count():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM base_coletas;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"[DB] Erro ao contar registros: {e}")
        return 0


# ================================================================
# FUNÇÕES DE NORMALIZAÇÃO
# ================================================================
def norm_text(s):
    s = unidecode(str(s)).upper()
    s = re.sub(r'[^\w\s]', ' ', s)
    return re.sub(r'\s+', ' ', s.strip())


def norm_cep(s):
    s = re.sub(r'\D', '', str(s))
    return s.zfill(8) if s else ''


# ================================================================
# PARSING DO ARQUIVO DO DIA
# ================================================================
def try_decode_bytes(b: bytes):
    for enc in ('utf-8', 'latin-1', 'cp1252'):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode('utf-8', errors='ignore')


def encontrar_linha_cabecalho(linhas):
    for i, l in enumerate(linhas):
        low = unidecode(l.lower())
        if 'remetente' in low and ('cep' in low or 'status coleta' in low):
            return i
    return 0


def split_linha(linha):
    if '\t' in linha:
        return [p.strip() for p in linha.split('\t') if p.strip()]
    return [p.strip() for p in re.split(r'\s{2,}', linha) if p.strip()]


def parse_txt_to_df(path_or_bytes, is_bytes=False):
    if is_bytes:
        text = try_decode_bytes(path_or_bytes)
    else:
        with open(path_or_bytes, 'rb') as f:
            text = try_decode_bytes(f.read())

    linhas = [l.strip() for l in text.splitlines() if l.strip()]
    if not linhas:
        return pd.DataFrame()

    idx    = encontrar_linha_cabecalho(linhas)
    header = split_linha(linhas[idx])

    dados = []
    for l in linhas[idx + 1:]:
        cols = split_linha(l)
        if len(cols) >= 5:
            if len(cols) < len(header):
                cols += [''] * (len(header) - len(cols))
            dados.append(cols[:len(header)])

    df = pd.DataFrame(dados, columns=header)

    colmap = {}
    for c in df.columns:
        c_low = unidecode(c.lower())
        if 'remetent'        in c_low: colmap[c] = 'Remetente'
        elif 'destinat'      in c_low: colmap[c] = 'Destinatario'
        elif 'endereco orig' in c_low: colmap[c] = 'EnderecoOrigem'
        elif 'cep orig'      in c_low: colmap[c] = 'CEPOrigem'
        elif 'status coleta' in c_low: colmap[c] = 'StatusColeta'

    df = df.rename(columns=colmap)

    for need in ('Remetente', 'EnderecoOrigem', 'CEPOrigem', 'Destinatario', 'StatusColeta'):
        if need not in df.columns:
            df[need] = ''

    df['chave'] = (
        df['Remetente'].map(norm_text) + '|' +
        df['EnderecoOrigem'].map(norm_text) + '|' +
        df['CEPOrigem'].map(norm_cep)
    )
    return df


# ================================================================
# CONSULTA NO BANCO — busca todas as chaves de uma vez (1 query)
# ================================================================
def buscar_dados_por_chaves(chaves: list) -> dict:
    """
    Recebe uma lista de chaves e retorna um dict {chave: (numero_coleta, status_coleta)}.
    Faz UMA única query com ANY() — muito eficiente com índice.
    """
    if not chaves:
        return {}
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (chave) chave, numero_coleta, status_coleta
            FROM base_coletas
            WHERE chave = ANY(%s)
            """,
            (chaves,)
        )
        resultado = {row[0]: {'numero_coleta': row[1], 'status_coleta': row[2]} for row in cur.fetchall()}
        cur.close()
        conn.close()
        return resultado
    except Exception as e:
        print(f"[DB] Erro na consulta: {e}")
        return {}


# ================================================================
# ROTAS
# ================================================================

@app.route('/')
def index():
    base_count = get_base_count()
    return render_template('index.html', base_count=base_count)


@app.route('/upload_dia', methods=['POST'])
def upload_dia():
    global DF_MATCH

    f = request.files.get('file')
    if not f:
        return redirect(url_for('index'))

    # 1. Lê e processa o arquivo do dia
    df_dia = parse_txt_to_df(f.read(), is_bytes=True)

    # 2. Filtra apenas VIVO
    df_vivo = df_dia[df_dia['Destinatario'].str.contains("VIVO", case=False, na=False)].copy()
    total_vivo = len(df_vivo)

    # 3. Consulta o banco com UMA query (todas as chaves de uma vez)
    chaves = df_vivo['chave'].unique().tolist()
    dados_map = buscar_dados_por_chaves(chaves)

    # 4. Aplica o resultado
    df_vivo['NumeroColeta'] = df_vivo['chave'].map(
        lambda c: dados_map[c]['numero_coleta'] if c in dados_map else None
    )
    df_vivo['StatusColeta'] = df_vivo['chave'].map(
        lambda c: dados_map[c]['status_coleta'] if c in dados_map else None
    )
    df_final = df_vivo[df_vivo['StatusColeta'].notna()].copy()

    # 5. Ordena por CEP
    if not df_final.empty:
        df_final['CEP_SORT'] = df_final['CEPOrigem'].str.replace(r'\D', '', regex=True)
        df_final = df_final.sort_values(by='CEP_SORT').drop(columns=['CEP_SORT'])

    DF_MATCH = df_final.copy()

    # 6. Gera tabela HTML (com N° Coleta)
    tabela_html = DF_MATCH[['NumeroColeta', 'CEPOrigem', 'Remetente', 'EnderecoOrigem', 'StatusColeta']].to_html(
        classes='table table-sm table-striped table-bordered',
        index=False,
        header=["N° COLETA", "CEP", "REMETENTE", "ENDEREÇO", "STATUS (HISTÓRICO)"]
    )

    return render_template('resultado.html', table=tabela_html,
                           total_vivo=total_vivo, total_repetidos=len(DF_MATCH))


@app.route('/download_pdf')
def download_pdf():
    global DF_MATCH
    if DF_MATCH is None or DF_MATCH.empty:
        return redirect(url_for('index'))

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(190, 10, "Relatorio de Coletas Repetidas", ln=True, align='C')
    pdf.ln(5)

    # Cabeçalho com N° Coleta
    pdf.set_font("Arial", 'B', 8)
    pdf.cell(28, 7, "N  COLETA", 1)   # N° com acento removido pelo latin-1
    pdf.cell(25, 7, "CEP", 1)
    pdf.cell(45, 7, "REMETENTE", 1)
    pdf.cell(60, 7, "ENDERECO", 1)
    pdf.cell(32, 7, "STATUS", 1, 1)

    pdf.set_font("Arial", '', 7)
    for _, row in DF_MATCH.iterrows():
        pdf.cell(28, 6, str(row.get('NumeroColeta', ''))[:18], 1)
        pdf.cell(25, 6, str(row['CEPOrigem']), 1)
        pdf.cell(45, 6, unidecode(str(row['Remetente']))[:30], 1)
        pdf.cell(60, 6, unidecode(str(row['EnderecoOrigem']))[:45], 1)
        pdf.cell(32, 6, unidecode(str(row['StatusColeta']))[:22], 1)
        pdf.ln()

    out = BytesIO()
    pdf_str = pdf.output(dest='S').encode('latin-1', errors='ignore')
    out.write(pdf_str)
    out.seek(0)
    return send_file(out, as_attachment=True, download_name="repetidos.pdf")


if __name__ == "__main__":
    Timer(1, lambda: webbrowser.open("http://localhost:5000")).start()
    app.run(port=5000, debug=False)
