"""
Script de importação única: lê o base.txt e popula o banco Neon.
Execute apenas uma vez (ou quando atualizar a base).

Uso:
    python importar_base.py
"""

import os
import re
import sys
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERRO: variável DATABASE_URL não encontrada no .env")
    sys.exit(1)

# ================================================================
# FUNÇÕES DE PARSING
# ================================================================

def try_decode_bytes(b: bytes):
    for enc in ('utf-8', 'latin-1', 'cp1252'):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode('utf-8', errors='ignore')


def norm_text(s):
    s = unidecode(str(s)).upper()
    s = re.sub(r'[^\w\s]', ' ', s)
    return re.sub(r'\s+', ' ', s.strip())


def norm_cep(s):
    s = re.sub(r'\D', '', str(s))
    return s.zfill(8) if s else ''


def parse_txt(path):
    """
    Lê o arquivo exportado do sistema de coletas.

    Estrutura do arquivo (todos os formatos):
      - Linha 0: título do relatório + TAB + cabeçalho + TAB + primeiro registro
                 (tudo junto na mesma linha, separado por TAB)
      - Linhas 1+: registros normais com 15 colunas separadas por TAB

    Chave gerada: Remetente|CEP  (sem endereço — garante compatibilidade
    entre arquivos locais e nacionais, que trazem o endereço em formatos
    diferentes/truncados)
    """
    with open(path, 'rb') as f:
        text = try_decode_bytes(f.read())

    linhas = [l.strip() for l in text.splitlines() if l.strip()]
    if not linhas:
        return []

    # ------------------------------------------------------------------
    # Determina o número real de colunas pela primeira linha de dados
    # ------------------------------------------------------------------
    linha0_parts = linhas[0].split('\t')
    linha1_parts = linhas[1].split('\t') if len(linhas) > 1 else []
    n_cols = len(linha1_parts)  # deve ser 15

    # ------------------------------------------------------------------
    # Extrai o cabeçalho das primeiras n_cols partes da linha 0
    # ------------------------------------------------------------------
    header = linha0_parts[:n_cols]

    # Col [0] está contaminada com o título → renomear para "Coleta"
    header[0] = 'Coleta'

    # Col [-1] = "CEP Destino XXXXXXXX" → limpar e extrair nº do 1º pedido
    match_num = re.search(r'(\d{7,})', header[-1])
    first_pedido_num = match_num.group(1) if match_num else ''
    header[-1] = re.sub(r'\s+\d+.*$', '', header[-1]).strip()  # "CEP Destino"

    # ------------------------------------------------------------------
    # Recupera o 1º registro embutido na linha 0
    # ------------------------------------------------------------------
    first_row_extra = linha0_parts[n_cols:]
    if first_pedido_num and len(first_row_extra) == n_cols - 1:
        first_row = [first_pedido_num] + first_row_extra
    else:
        first_row = None

    # ------------------------------------------------------------------
    # Mapeamento de índices → nomes padronizados
    # ------------------------------------------------------------------
    colmap = {}
    for i, c in enumerate(header):
        c_low = unidecode(c.lower())
        if   'remetent'      in c_low: colmap[i] = 'Remetente'
        elif 'destinat'      in c_low: colmap[i] = 'Destinatario'
        elif 'endereco orig' in c_low: colmap[i] = 'EnderecoOrigem'
        elif 'cep orig'      in c_low: colmap[i] = 'CEPOrigem'
        elif 'status coleta' in c_low: colmap[i] = 'StatusColeta'

    # ------------------------------------------------------------------
    # Monta os registros
    # ------------------------------------------------------------------
    def processar_cols(cols):
        if len(cols) < 5:
            return None
        if len(cols) < n_cols:
            cols += [''] * (n_cols - len(cols))
        cols = cols[:n_cols]
        row = {v: cols[k] for k, v in colmap.items()}
        for need in ('Remetente', 'EnderecoOrigem', 'CEPOrigem', 'Destinatario', 'StatusColeta'):
            if need not in row:
                row[need] = ''
        # Chave: Remetente + CEP (sem endereço — compatível com todos os formatos)
        row['chave'] = norm_text(row['Remetente']) + '|' + norm_cep(row['CEPOrigem'])
        return row

    registros = []

    if first_row:
        r = processar_cols(first_row)
        if r:
            registros.append(r)

    for l in linhas[1:]:
        cols = l.split('\t')
        r = processar_cols(cols)
        if r:
            registros.append(r)

    return registros


# ================================================================
# IMPORTAÇÃO PARA O NEON
# ================================================================

def importar(path_txt):
    print(f"Lendo {path_txt} ...")
    registros = parse_txt(path_txt)
    total = len(registros)
    print(f"  {total} registros encontrados.")

    if total == 0:
        print("Nenhum registro para importar.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Limpa a tabela antes de reimportar
    print("Limpando tabela anterior...")
    cur.execute("TRUNCATE TABLE base_coletas RESTART IDENTITY;")

    # Insere em lotes de 5000
    BATCH = 5000
    print(f"Importando em lotes de {BATCH}...")
    for i in range(0, total, BATCH):
        lote = registros[i:i + BATCH]
        valores = [
            (
                r['Remetente'],
                r['EnderecoOrigem'],
                r['CEPOrigem'],
                r['Destinatario'],
                r['StatusColeta'],
                r['chave'],
            )
            for r in lote
        ]
        execute_values(cur,
            """
            INSERT INTO base_coletas
                (remetente, endereco_origem, cep_origem, destinatario, status_coleta, chave)
            VALUES %s
            """,
            valores
        )
        print(f"  {min(i + BATCH, total)}/{total} registros inseridos...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nImportação concluída! {total} registros no banco.")


if __name__ == "__main__":
    if getattr(sys, 'frozen', False):
        BASE_DIR = os.path.dirname(sys.executable)
    else:
        BASE_DIR = os.path.abspath(".")

    base_txt = os.path.join(BASE_DIR, "data", "base.txt")

    if not os.path.exists(base_txt):
        print(f"ERRO: arquivo não encontrado em {base_txt}")
        sys.exit(1)

    importar(base_txt)
