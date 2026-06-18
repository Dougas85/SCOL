import os
import re
from io import BytesIO
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
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

# DF_MATCH guarda o resultado completo (sem filtro de CEP)
# DF_FILTERED guarda o resultado após filtro de CEP (pode ser igual ao completo)
DF_MATCH    = None
DF_FILTERED = None


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


def parse_txt_to_df(path_or_bytes, is_bytes=False):
    """
    Lê o arquivo exportado do sistema de coletas.

    Estrutura do arquivo (ambos os formatos):
      - Linha 0: título do relatório + TAB + colunas do cabeçalho + TAB + primeiro registro
                 (tudo na mesma linha, separado por TAB)
      - Linhas 1+: registros normais com 15 colunas separadas por TAB

    Suporta dois formatos de data:
      - Formato 1 (local):    DD/MM/AAAA-HH:MM       ex: 16/06/2026-10:08
      - Formato 2 (nacional): AAAA-MM-DD HH:MM:SS.f  ex: 2026-06-13 00:15:28.0
    """
    if is_bytes:
        text = try_decode_bytes(path_or_bytes)
    else:
        with open(path_or_bytes, 'rb') as f:
            text = try_decode_bytes(f.read())

    linhas = [l.strip() for l in text.splitlines() if l.strip()]
    if not linhas:
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Determina o número real de colunas a partir da primeira linha de dados
    # ------------------------------------------------------------------
    linha0_parts = linhas[0].split('\t')
    linha1_parts = linhas[1].split('\t') if len(linhas) > 1 else []
    n_cols = len(linha1_parts)  # deve ser 15

    # ------------------------------------------------------------------
    # Extrai o cabeçalho: primeiras n_cols partes de linha0
    # ------------------------------------------------------------------
    header = linha0_parts[:n_cols]

    # Col [0] está contaminada com o título do relatório → renomear para "Coleta"
    header[0] = 'Coleta'

    # Col [-1] = "CEP Destino XXXXXXXX" (número do 1º pedido colado)
    # → extrair o número e limpar o nome da coluna
    match_num = re.search(r'(\d{7,})', header[-1])
    first_pedido_num = match_num.group(1) if match_num else ''
    header[-1] = re.sub(r'\s+\d+.*$', '', header[-1]).strip()  # "CEP Destino"

    # ------------------------------------------------------------------
    # Recupera o primeiro registro que ficou embutido na linha 0
    # ------------------------------------------------------------------
    first_row_extra = linha0_parts[n_cols:]  # 14 partes (sem o número do pedido)
    if first_pedido_num and len(first_row_extra) == n_cols - 1:
        first_row = [first_pedido_num] + first_row_extra
    else:
        first_row = None  # estrutura inesperada — descarta

    # ------------------------------------------------------------------
    # Monta os dados
    # ------------------------------------------------------------------
    dados = []
    if first_row:
        dados.append(first_row)
    for l in linhas[1:]:
        cols = l.split('\t')
        if len(cols) >= 5:
            if len(cols) < n_cols:
                cols += [''] * (n_cols - len(cols))
            dados.append(cols[:n_cols])

    df = pd.DataFrame(dados, columns=header)

    # ------------------------------------------------------------------
    # Mapeamento de colunas para nomes padronizados
    # ------------------------------------------------------------------
    colmap = {}
    for c in df.columns:
        c_low = unidecode(c.lower())
        if   'remetent'      in c_low: colmap[c] = 'Remetente'
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
# HELPER — gera tabela HTML padronizada
# ================================================================
def df_to_html(df: pd.DataFrame) -> str:
    return df[['NumeroColeta', 'CEPOrigem', 'Remetente', 'EnderecoOrigem', 'StatusColeta']].to_html(
        classes='table table-sm table-striped table-bordered',
        index=False,
        header=["N° COLETA", "CEP", "REMETENTE", "ENDEREÇO", "STATUS (HISTÓRICO)"]
    )


# ================================================================
# ROTAS
# ================================================================

@app.route('/')
def index():
    base_count = get_base_count()
    return render_template('index.html', base_count=base_count)


@app.route('/upload_dia', methods=['POST'])
def upload_dia():
    global DF_MATCH, DF_FILTERED

    f = request.files.get('file')
    if not f:
        return redirect(url_for('index'))

    # 1. Lê e processa o arquivo do dia
    df_dia = parse_txt_to_df(f.read(), is_bytes=True)

    # 2. Filtra apenas VIVO
    df_vivo = df_dia[df_dia['Destinatario'].str.contains("VIVO", case=False, na=False)].copy()
    total_vivo = len(df_vivo)

    # 3. Consulta o banco com UMA query
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

    # Guarda o resultado completo (sem filtro de CEP ainda)
    DF_MATCH    = df_final.copy()
    DF_FILTERED = df_final.copy()

    tabela_html = df_to_html(DF_FILTERED)

    return render_template(
        'resultado.html',
        table=tabela_html,
        total_vivo=total_vivo,
        total_repetidos=len(DF_FILTERED),
        cep_min=None,
        cep_max=None,
    )


@app.route('/filtrar_cep', methods=['POST'])
def filtrar_cep():
    """
    Recebe cep_min e cep_max via POST, refiltra DF_MATCH em memória
    e devolve a página de resultado com a tabela filtrada.
    """
    global DF_MATCH, DF_FILTERED

    if DF_MATCH is None or DF_MATCH.empty:
        return redirect(url_for('index'))

    cep_min_raw = request.form.get('cep_min', '').strip()
    cep_max_raw = request.form.get('cep_max', '').strip()

    # Normaliza: remove tudo que não é dígito e preenche com zeros
    cep_min_num = norm_cep(cep_min_raw)
    cep_max_num = norm_cep(cep_max_raw)

    if not cep_min_num or not cep_max_num:
        flash("Informe os dois CEPs para filtrar.", "warning")
        return redirect(url_for('index'))

    # Filtra pelo CEP numérico (8 dígitos, zero-padded)
    df = DF_MATCH.copy()
    df['_cep_num'] = df['CEPOrigem'].apply(norm_cep)
    DF_FILTERED = df[
        (df['_cep_num'] >= cep_min_num) & (df['_cep_num'] <= cep_max_num)
    ].drop(columns=['_cep_num']).copy()

    tabela_html = df_to_html(DF_FILTERED)

    # Formata para exibição (com hífen)
    def fmt_cep(c):
        c = re.sub(r'\D', '', c).zfill(8)
        return f"{c[:5]}-{c[5:]}" if len(c) == 8 else c

    return render_template(
        'resultado.html',
        table=tabela_html,
        total_vivo=len(DF_MATCH),          # total geral (sem filtro)
        total_repetidos=len(DF_FILTERED),  # total filtrado
        cep_min=fmt_cep(cep_min_raw),
        cep_max=fmt_cep(cep_max_raw),
    )


@app.route('/limpar_filtro')
def limpar_filtro():
    """Remove o filtro de CEP e volta a exibir todos os registros."""
    global DF_MATCH, DF_FILTERED

    if DF_MATCH is None or DF_MATCH.empty:
        return redirect(url_for('index'))

    DF_FILTERED = DF_MATCH.copy()
    tabela_html = df_to_html(DF_FILTERED)

    return render_template(
        'resultado.html',
        table=tabela_html,
        total_vivo=len(DF_MATCH),
        total_repetidos=len(DF_FILTERED),
        cep_min=None,
        cep_max=None,
    )


@app.route('/download_pdf')
def download_pdf():
    global DF_FILTERED
    if DF_FILTERED is None or DF_FILTERED.empty:
        return redirect(url_for('index'))

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(190, 10, "Relatorio de Coletas Repetidas", ln=True, align='C')
    pdf.ln(5)

    pdf.set_font("Arial", 'B', 8)
    pdf.cell(28, 7, "N  COLETA", 1)
    pdf.cell(25, 7, "CEP", 1)
    pdf.cell(45, 7, "REMETENTE", 1)
    pdf.cell(60, 7, "ENDERECO", 1)
    pdf.cell(32, 7, "STATUS", 1, 1)

    pdf.set_font("Arial", '', 7)
    for _, row in DF_FILTERED.iterrows():
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
