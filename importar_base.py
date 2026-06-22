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
# FUNÇÕES DE NORMALIZAÇÃO
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


# ================================================================
# PARSING — detecta formato automaticamente
# ================================================================

def parse_txt(path):
    """
    Detecta automaticamente o formato do arquivo e faz o parse correto.

    FORMATO A — linha 0 contém título + cabeçalho + 1º registro tudo junto (≥14 tabs).
    FORMATO B — título em linhas separadas; cabeçalho em linha própria com 14 tabs.

    Chave: Remetente|CEP — compatível com endereços completos ou truncados.
    """
    with open(path, 'rb') as f:
        text = try_decode_bytes(f.read())

    todas = text.splitlines()
    if not todas:
        return []

    linha0_tabs = todas[0].count('\t')

    if linha0_tabs >= 14:
        # ----------------------------------------------------------
        # FORMATO A: tudo na linha 0
        # ----------------------------------------------------------
        linhas = [l.strip() for l in todas if l.strip()]
        linha0_parts = linhas[0].split('\t')
        linha1_parts = linhas[1].split('\t') if len(linhas) > 1 else []
        n_cols = len(linha1_parts)

        header = linha0_parts[:n_cols]
        header[0] = 'Coleta'
        match_num = re.search(r'(\d{7,})', header[-1])
        first_pedido_num = match_num.group(1) if match_num else ''
        header[-1] = re.sub(r'\s+\d+.*$', '', header[-1]).strip()

        first_row_extra = linha0_parts[n_cols:]
        first_row = [first_pedido_num] + first_row_extra \
            if first_pedido_num and len(first_row_extra) == n_cols - 1 else None

        colmap = {}
        for i, c in enumerate(header):
            c_low = unidecode(c.lower())
            if   c_low == 'coleta':        colmap[i] = 'NumeroColeta'
            elif 'remetent'    in c_low:   colmap[i] = 'Remetente'
            elif 'destinat'    in c_low:   colmap[i] = 'Destinatario'
            elif 'endereco orig' in c_low: colmap[i] = 'EnderecoOrigem'
            elif 'cep orig'    in c_low:   colmap[i] = 'CEPOrigem'
            elif 'status coleta' in c_low: colmap[i] = 'StatusColeta'

        def processar(cols):
            if len(cols) < 5: return None
            if len(cols) < n_cols: cols += [''] * (n_cols - len(cols))
            cols = cols[:n_cols]
            row = {v: cols[k] for k, v in colmap.items()}
            for need in ('NumeroColeta', 'Remetente', 'EnderecoOrigem', 'CEPOrigem', 'Destinatario', 'StatusColeta'):
                if need not in row: row[need] = ''
            row['chave'] = norm_text(row['Remetente']) + '|' + norm_cep(row['CEPOrigem'])
            return row

        registros = []
        if first_row:
            r = processar(first_row)
            if r: registros.append(r)
        for l in linhas[1:]:
            r = processar(l.split('\t'))
            if r: registros.append(r)

    else:
        # ----------------------------------------------------------
        # FORMATO B: cabeçalho em linha própria (14 tabs)
        # ----------------------------------------------------------
        cab_idx = None
        for i, l in enumerate(todas):
            if l.count('\t') >= 14 and 'coleta' in unidecode(l.lower()):
                cab_idx = i
                break

        if cab_idx is None:
            return []

        header = todas[cab_idx].split('\t')
        n_cols = len(header)

        colmap = {}
        for i, c in enumerate(header):
            c_low = unidecode(c.lower())
            if   c_low == 'coleta':        colmap[i] = 'NumeroColeta'
            elif 'remetent'    in c_low:   colmap[i] = 'Remetente'
            elif 'destinat'    in c_low:   colmap[i] = 'Destinatario'
            elif 'endereco orig' in c_low: colmap[i] = 'EnderecoOrigem'
            elif 'cep orig'    in c_low:   colmap[i] = 'CEPOrigem'
            elif 'status coleta' in c_low: colmap[i] = 'StatusColeta'

        def processar(cols):
            if len(cols) < 5: return None
            if len(cols) < n_cols: cols += [''] * (n_cols - len(cols))
            cols = cols[:n_cols]
            row = {v: cols[k] for k, v in colmap.items()}
            for need in ('NumeroColeta', 'Remetente', 'EnderecoOrigem', 'CEPOrigem', 'Destinatario', 'StatusColeta'):
                if need not in row: row[need] = ''
            row['chave'] = norm_text(row['Remetente']) + '|' + norm_cep(row['CEPOrigem'])
            return row

        registros = []
        for l in todas[cab_idx + 1:]:
            if not l.strip(): continue
            r = processar(l.split('\t'))
            if r: registros.append(r)

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

    print("Limpando tabela anterior...")
    cur.execute("TRUNCATE TABLE base_coletas RESTART IDENTITY;")

    BATCH = 5000
    print(f"Importando em lotes de {BATCH}...")
    for i in range(0, total, BATCH):
        lote = registros[i:i + BATCH]
        valores = [
            (
                r['NumeroColeta'],
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
                (numero_coleta, remetente, endereco_origem, cep_origem, destinatario, status_coleta, chave)
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
