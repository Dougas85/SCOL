# 📦 Sistema de Cruzamento de Coletas — 

Sistema web desenvolvido em Python/Flask para cruzamento automático de coletas diárias de ** Determinado Cliente** com uma base histórica, identificando endereços repetidos.

---

## 🚀 Funcionalidades

- Upload de arquivo diário (`.txt` ou `.csv`) via interface web
- Cruzamento automático com base histórica de até 300k+ registros
- Filtragem automática de coletas **Do cliente**
- Identificação de endereços repetidos com status histórico
- Exportação dos resultados em **PDF**
- Interface moderna e responsiva
- Banco de dados **PostgreSQL (Neon)** para produção

---

## 🗂️ Estrutura do Projeto

```
scol/
├── coletas.py          # Aplicação principal Flask
├── importar_base.py    # Script de importação da base histórica (rodar uma vez)
├── requirements.txt    # Dependências Python
├── .env                # Variáveis de ambiente (não versionar)
├── .gitignore
├── data/
│   └── base.txt        # Base histórica em formato TXT
└── templates/
    ├── index.html      # Página principal
    └── resultado.html  # Página de resultados
```

---

## ⚙️ Pré-requisitos

- Python 3.10+
- Conta no [Neon](https://neon.tech) (banco PostgreSQL gratuito)
- Conta no [Render](https://render.com) para deploy (opcional)

---

## 🔧 Instalação Local

**1. Clone o repositório:**
```bash
git clone https://github.com/seu-usuario/scol.git
cd scol
```

**2. Instale as dependências:**
```bash
pip install -r requirements.txt
```

**3. Configure o arquivo `.env`** na raiz do projeto:
```
DATABASE_URL=postgresql://usuario:senha@ep-xxxx.neon.tech/neondb?sslmode=require
```

**4. Crie a tabela no banco** (execute no SQL Editor do Neon):
```sql
CREATE TABLE IF NOT EXISTS base_coletas (
    id SERIAL PRIMARY KEY,
    remetente TEXT,
    endereco_origem TEXT,
    cep_origem TEXT,
    destinatario TEXT,
    status_coleta TEXT,
    chave TEXT
);

CREATE INDEX IF NOT EXISTS idx_chave ON base_coletas(chave);
```

**5. Importe a base histórica** (rodar apenas uma vez ou ao atualizar a base):
```bash
python importar_base.py
```

**6. Inicie a aplicação:**
```bash
python coletas.py
```

O navegador abrirá automaticamente em `http://localhost:5000`.

---

## 📋 Como Usar

1. Acesse a interface web
2. Verifique o total de registros da base histórica carregada
3. Faça o upload do arquivo `.txt` ou `.csv` do dia
4. Clique em **Processar Arquivo**
5. Visualize os endereços  repetidos, ordenados por CEP
6. Baixe o relatório em **PDF** se necessário

---

## 🗄️ Banco de Dados

O sistema utiliza **PostgreSQL via Neon** em produção. A chave de cruzamento é composta por:

```
chave = REMETENTE_NORMALIZADO | ENDERECO_NORMALIZADO | CEP_NORMALIZADO
```

A normalização remove acentos, converte para maiúsculas e padroniza espaços, garantindo correspondências mesmo com pequenas variações de digitação.

---

## 🌐 Deploy no Render

1. Suba o projeto para o GitHub (sem o `.env` e sem a pasta `data/`)
2. Crie um novo **Web Service** no Render apontando para o repositório
3. Configure a variável de ambiente `DATABASE_URL` no painel do Render
4. O Render instalará as dependências via `requirements.txt` automaticamente

---

## 📦 Dependências

```
flask
pandas
fpdf
unidecode
psycopg2-binary
python-dotenv
```

---

## 🔒 Segurança

- O arquivo `.env` **nunca deve ser versionado** — ele contém credenciais do banco
- A pasta `data/` também está no `.gitignore` pois contém dados sensíveis de coletas

---

## 👨‍💻 Desenvolvido por

**DFS** — © 2026. Todos os direitos reservados.
