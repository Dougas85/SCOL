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
# FUNÇÕES DE PARSING (mesmas do coletas.py)
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


def parse_txt(path):
    with open(path, 'rb') as f:
        text = try_decode_bytes(f.read())

    linhas = [l.strip() for l in text.splitlines() if l.strip()]
    if not linhas:
        return []

    idx    = encontrar_linha_cabecalho(linhas)
    header = split_linha(linhas[idx])

    colmap = {}
    for i, c in enumerate(header):
        c_low = unidecode(c.lower())
        if 'remetent'      in c_low: colmap[i] = 'Remetente'
        elif 'destinat'    in c_low: colmap[i] = 'Destinatario'
        elif 'endereco orig' in c_low: colmap[i] = 'EnderecoOrigem'
        elif 'cep orig'    in c_low: colmap[i] = 'CEPOrigem'
        elif 'status coleta' in c_low: colmap[i] = 'StatusColeta'

    registros = []
    for l in linhas[idx + 1:]:
        cols = split_linha(l)
        if len(cols) < 5:
            continue
        if len(cols) < len(header):
            cols += [''] * (len(header) - len(cols))
        cols = cols[:len(header)]

        row = {v: cols[k] for k, v in colmap.items()}
        for need in ('Remetente', 'EnderecoOrigem', 'CEPOrigem', 'Destinatario', 'StatusColeta'):
            if need not in row:
                row[need] = ''

        row['chave'] = (
            norm_text(row['Remetente']) + '|' +
            norm_text(row['EnderecoOrigem']) + '|' +
            norm_cep(row['CEPOrigem'])
        )
        registros.append(row)

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

    # Insere em lotes de 5000 (eficiente para 300k linhas)
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
                r['chave']
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
    # Localiza o base.txt
    if getattr(sys, 'frozen', False):
        BASE_DIR = os.path.dirname(sys.executable)
    else:
        BASE_DIR = os.path.abspath(".")

    base_txt = os.path.join(BASE_DIR, "data", "base.txt")

    if not os.path.exists(base_txt):
        print(f"ERRO: arquivo não encontrado em {base_txt}")
        sys.exit(1)

    importar(base_txt)