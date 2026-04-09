"""
Sistema de Prospecção MEI — Script de atualização automática
Baixa bases públicas da Receita Federal e PGFN, cruza os dados
e gera o arquivo JSON usado pela interface web.
"""

import os
import re
import csv
import json
import zipfile
import sqlite3
import logging
import requests
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DOCS_DIR = BASE_DIR / "docs"
DB_PATH  = DATA_DIR / "prospeccao.db"

DATA_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

# ── URLs das bases públicas ──────────────────────────────────────────────────
# Receita Federal — CNPJ (arquivo de empresas, ~2 GB, atualizado mensalmente)
# Cada arquivo cobre uma faixa; para MEI filtramos OPCAO_PELO_MEI = "S"
RF_CNPJ_URLS = [
    "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0_csv.zip",
    "https://dadosabertos.rfb.gov.br/CNPJ/Empresas1_csv.zip",
    # Adicione Empresas2..9 se quiser cobertura nacional completa
]

RF_MEI_URL  = "https://dadosabertos.rfb.gov.br/CNPJ/Simples.zip"

# Estabelecimentos (endereço, município, situação cadastral)
RF_ESTAB_URLS = [
    "https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos0_csv.zip",
    "https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos1_csv.zip",
]

# PGFN — Dívida Ativa
PGFN_MEI_URL = "https://www.portaldatransparencia.gov.br/download-de-dados/pgfn/mei"

# Código IBGE dos municípios pré-carregados
MUNICIPIOS = {
    "2051": "João Pessoa",
    "2110": "Campina Grande",
    "2180": "Patos",
    "2090": "Santa Rita",
    "0001": "São Paulo",
    "0013": "Rio de Janeiro",
}


# ── Helpers ──────────────────────────────────────────────────────────────────
def baixar_arquivo(url: str, destino: Path, chunk_mb: int = 8) -> bool:
    """Baixa um arquivo com barra de progresso simples."""
    if destino.exists():
        log.info(f"  já existe: {destino.name} — pulando download")
        return True
    log.info(f"  baixando {url}")
    try:
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        baixado = 0
        with open(destino, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                f.write(chunk)
                baixado += len(chunk)
                if total:
                    pct = baixado / total * 100
                    print(f"\r  {pct:5.1f}%  {baixado//1024//1024} MB", end="", flush=True)
        print()
        log.info(f"  salvo em {destino}")
        return True
    except Exception as e:
        log.error(f"  falha no download: {e}")
        if destino.exists():
            destino.unlink()
        return False


def extrair_zip(zip_path: Path, destino_dir: Path) -> list[Path]:
    """Extrai ZIP e retorna lista de arquivos CSV extraídos."""
    arquivos = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for nome in z.namelist():
            if nome.lower().endswith(".csv") or nome.lower().endswith(".txt"):
                z.extract(nome, destino_dir)
                arquivos.append(destino_dir / nome)
    return arquivos


def detectar_sep(linha: str) -> str:
    return ";" if linha.count(";") > linha.count(",") else ","


# ── Banco de dados ────────────────────────────────────────────────────────────
def criar_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS empresas (
        cnpj_basico   TEXT PRIMARY KEY,
        razao_social  TEXT,
        natureza_jur  TEXT,
        porte         TEXT,
        opcao_mei     TEXT   -- S ou N
    );

    CREATE TABLE IF NOT EXISTS estabelecimentos (
        cnpj_basico   TEXT,
        cnpj_ordem    TEXT,
        cnpj_dv       TEXT,
        sit_cadastral TEXT,  -- 01=Nula,02=Ativa,03=Suspensa,04=Inapta,08=Baixada
        municipio     TEXT,
        uf            TEXT,
        ddd1          TEXT,
        telefone1     TEXT,
        email         TEXT,
        PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
    );

    CREATE TABLE IF NOT EXISTS divida_ativa (
        cnpj          TEXT PRIMARY KEY,
        nome_devedor  TEXT,
        valor_total   REAL,
        situacao      TEXT,
        tipo_pessoa   TEXT,
        uf_devedor    TEXT
    );

    CREATE TABLE IF NOT EXISTS simples (
        cnpj_basico   TEXT PRIMARY KEY,
        opcao_simples TEXT,
        opcao_mei     TEXT,
        dt_exclusao   TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_estab_mun ON estabelecimentos(municipio);
    CREATE INDEX IF NOT EXISTS idx_estab_uf  ON estabelecimentos(uf);
    CREATE INDEX IF NOT EXISTS idx_div_cnpj  ON divida_ativa(cnpj);
    """)
    conn.commit()


# ── Importação das bases ──────────────────────────────────────────────────────
def importar_empresas(conn: sqlite3.Connection, csv_path: Path):
    log.info(f"Importando empresas: {csv_path.name}")
    count = 0
    with open(csv_path, encoding="latin-1", errors="replace") as f:
        sep = detectar_sep(f.readline())
        f.seek(0)
        reader = csv.reader(f, delimiter=sep)
        batch = []
        for row in reader:
            if len(row) < 7:
                continue
            batch.append((
                row[0].strip(),   # cnpj_basico
                row[1].strip(),   # razao_social
                row[2].strip(),   # natureza_juridica
                row[6].strip(),   # porte
                "",               # opcao_mei (será atualizado via Simples.zip)
            ))
            if len(batch) >= 5000:
                conn.executemany(
                    "INSERT OR REPLACE INTO empresas VALUES (?,?,?,?,?)", batch
                )
                conn.commit()
                count += len(batch)
                batch = []
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO empresas VALUES (?,?,?,?,?)", batch
            )
            conn.commit()
            count += len(batch)
    log.info(f"  {count} empresas importadas")


def importar_simples(conn: sqlite3.Connection, csv_path: Path):
    """Importa arquivo Simples.csv — contém flag MEI por cnpj_basico."""
    log.info(f"Importando Simples/MEI: {csv_path.name}")
    count = 0
    with open(csv_path, encoding="latin-1", errors="replace") as f:
        sep = detectar_sep(f.readline())
        f.seek(0)
        reader = csv.reader(f, delimiter=sep)
        batch = []
        for row in reader:
            if len(row) < 6:
                continue
            cnpj  = row[0].strip()
            opt_s = row[1].strip()  # S/N Simples
            opt_m = row[3].strip()  # S/N MEI
            dt_ex = row[5].strip()  # data exclusão
            batch.append((cnpj, opt_s, opt_m, dt_ex))
            if len(batch) >= 5000:
                conn.executemany(
                    "INSERT OR REPLACE INTO simples VALUES (?,?,?,?)", batch
                )
                conn.commit()
                count += len(batch)
                batch = []
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO simples VALUES (?,?,?,?)", batch
            )
            conn.commit()
            count += len(batch)
    # Sincroniza flag MEI na tabela empresas
    conn.execute("""
        UPDATE empresas SET opcao_mei = (
            SELECT opcao_mei FROM simples WHERE simples.cnpj_basico = empresas.cnpj_basico
        )
        WHERE EXISTS (
            SELECT 1 FROM simples WHERE simples.cnpj_basico = empresas.cnpj_basico
        )
    """)
    conn.commit()
    log.info(f"  {count} registros Simples/MEI importados")


def importar_estabelecimentos(conn: sqlite3.Connection, csv_path: Path):
    log.info(f"Importando estabelecimentos: {csv_path.name}")
    count = 0
    with open(csv_path, encoding="latin-1", errors="replace") as f:
        sep = detectar_sep(f.readline())
        f.seek(0)
        reader = csv.reader(f, delimiter=sep)
        batch = []
        for row in reader:
            if len(row) < 20:
                continue
            batch.append((
                row[0].strip(),   # cnpj_basico
                row[1].strip(),   # cnpj_ordem
                row[2].strip(),   # cnpj_dv
                row[5].strip(),   # sit_cadastral
                row[20].strip(),  # municipio (código IBGE 4 dígitos)
                row[19].strip(),  # uf
                row[12].strip(),  # ddd1
                row[13].strip(),  # telefone1
                row[27].strip() if len(row) > 27 else "",  # email
            ))
            if len(batch) >= 5000:
                conn.executemany(
                    "INSERT OR REPLACE INTO estabelecimentos VALUES (?,?,?,?,?,?,?,?,?)",
                    batch
                )
                conn.commit()
                count += len(batch)
                batch = []
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO estabelecimentos VALUES (?,?,?,?,?,?,?,?,?)",
                batch
            )
            conn.commit()
            count += len(batch)
    log.info(f"  {count} estabelecimentos importados")


def importar_divida_ativa(conn: sqlite3.Connection, csv_path: Path):
    """Importa arquivo PGFN de dívida ativa."""
    log.info(f"Importando dívida ativa: {csv_path.name}")
    count = 0
    with open(csv_path, encoding="latin-1", errors="replace") as f:
        sep = detectar_sep(f.readline())
        f.seek(0)
        reader = csv.DictReader(f, delimiter=sep)
        batch = []
        for row in reader:
            # Campos variam conforme versão do arquivo PGFN
            cnpj   = re.sub(r"\D", "", row.get("CPF_CNPJ", row.get("CNPJ_CPF", "")))
            nome   = row.get("NOME_DEVEDOR", row.get("NOME", "")).strip()
            try:
                valor  = float(
                    row.get("VALOR_TOTAL", row.get("VALOR_CONSOLIDADO", "0"))
                    .replace(".", "").replace(",", ".").strip() or "0"
                )
            except ValueError:
                valor = 0.0
            sit    = row.get("SITUACAO_INSCRICAO", row.get("SITUACAO", "")).strip()
            tp     = row.get("TIPO_PESSOA", "").strip()
            uf     = row.get("UF_DEVEDOR", row.get("UF", "")).strip()
            if not cnpj:
                continue
            batch.append((cnpj, nome, valor, sit, tp, uf))
            if len(batch) >= 5000:
                conn.executemany(
                    "INSERT OR REPLACE INTO divida_ativa VALUES (?,?,?,?,?,?)", batch
                )
                conn.commit()
                count += len(batch)
                batch = []
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO divida_ativa VALUES (?,?,?,?,?,?)", batch
            )
            conn.commit()
            count += len(batch)
    log.info(f"  {count} registros de dívida ativa importados")


# ── Geração do JSON para o frontend ──────────────────────────────────────────
def gerar_json_por_municipio(conn: sqlite3.Connection):
    """
    Para cada município configurado, gera um JSON com os MEIs
    cruzados com dívida ativa, prontos para o frontend consumir.
    """
    for cod_mun, nome_mun in MUNICIPIOS.items():
        log.info(f"Gerando JSON para {nome_mun} ({cod_mun})")

        rows = conn.execute("""
            SELECT
                e.cnpj_basico,
                est.cnpj_ordem,
                est.cnpj_dv,
                emp.razao_social,
                emp.opcao_mei,
                est.sit_cadastral,
                est.uf,
                est.ddd1,
                est.telefone1,
                est.email,
                d.valor_total,
                d.situacao AS sit_divida
            FROM estabelecimentos est
            JOIN empresas emp ON emp.cnpj_basico = est.cnpj_basico
            LEFT JOIN divida_ativa d
                   ON d.cnpj = (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv)
                   OR d.cnpj = est.cnpj_basico
            WHERE est.municipio = ?
              AND emp.opcao_mei = 'S'
            LIMIT 5000
        """, (cod_mun,)).fetchall()

        registros = []
        for r in rows:
            (cnpj_b, cnpj_o, cnpj_dv, razao, mei, sit_cad,
             uf, ddd, tel, email, valor, sit_div) = r

            cnpj_fmt = f"{cnpj_b[:2]}.{cnpj_b[2:5]}.{cnpj_b[5:]}/{cnpj_o}-{cnpj_dv}"
            tel_num = f"{ddd}{tel}".strip() if tel else ""

            sit_map = {"02": "Ativa", "03": "Suspensa", "04": "Inapta", "08": "Baixada"}
            status = sit_map.get(sit_cad, "Desconhecida")

            registros.append({
                "cnpj":      cnpj_fmt,
                "cnpj_raw":  cnpj_b + cnpj_o + cnpj_dv,
                "nome":      razao,
                "mei":       mei == "S",
                "ativo":     sit_cad == "02",
                "status":    status,
                "uf":        uf,
                "divida":    valor is not None and valor > 0,
                "valor":     round(valor or 0, 2),
                "sit_divida": sit_div or "",
                "das":       True,   # será cruzado com base DAS quando disponível
                "irr":       valor is not None and valor > 0,
                "tel":       tel_num,
                "email":     email or "",
                "wa_link":   f"https://wa.me/55{tel_num}" if tel_num else "",
            })

        saida = {
            "municipio":    nome_mun,
            "codigo_ibge":  cod_mun,
            "gerado_em":    datetime.now().isoformat(),
            "total":        len(registros),
            "registros":    registros,
        }

        json_path = DOCS_DIR / f"dados_{cod_mun}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(saida, f, ensure_ascii=False, separators=(",", ":"))
        log.info(f"  → {json_path} ({len(registros)} registros)")

    # JSON índice de municípios
    indice = [
        {"codigo": k, "nome": v, "arquivo": f"dados_{k}.json"}
        for k, v in MUNICIPIOS.items()
    ]
    with open(DOCS_DIR / "municipios.json", "w", encoding="utf-8") as f:
        json.dump(indice, f, ensure_ascii=False, indent=2)
    log.info("JSON de municípios gerado.")


# ── Pipeline principal ────────────────────────────────────────────────────────
def main():
    log.info("═══════════════════════════════════════════")
    log.info("  Prospecção MEI — atualização de dados")
    log.info(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("═══════════════════════════════════════════")

    conn = sqlite3.connect(DB_PATH)
    criar_schema(conn)

    # 1. Baixar e importar Simples/MEI (arquivo menor ~100 MB)
    simples_zip = DATA_DIR / "Simples.zip"
    if baixar_arquivo(RF_MEI_URL, simples_zip):
        csvs = extrair_zip(simples_zip, DATA_DIR)
        for csv_f in csvs:
            importar_simples(conn, csv_f)
            csv_f.unlink(missing_ok=True)

    # 2. Baixar e importar Empresas (nome, porte, etc.)
    for url in RF_CNPJ_URLS:
        nome = url.split("/")[-1]
        zip_path = DATA_DIR / nome
        if baixar_arquivo(url, zip_path):
            csvs = extrair_zip(zip_path, DATA_DIR)
            for csv_f in csvs:
                importar_empresas(conn, csv_f)
                csv_f.unlink(missing_ok=True)

    # 3. Baixar e importar Estabelecimentos (endereço, município)
    for url in RF_ESTAB_URLS:
        nome = url.split("/")[-1]
        zip_path = DATA_DIR / nome
        if baixar_arquivo(url, zip_path):
            csvs = extrair_zip(zip_path, DATA_DIR)
            for csv_f in csvs:
                importar_estabelecimentos(conn, csv_f)
                csv_f.unlink(missing_ok=True)

    # 4. Baixar e importar dívida ativa PGFN
    pgfn_zip = DATA_DIR / "pgfn_mei.zip"
    if baixar_arquivo(PGFN_MEI_URL, pgfn_zip):
        csvs = extrair_zip(pgfn_zip, DATA_DIR)
        for csv_f in csvs:
            importar_divida_ativa(conn, csv_f)
            csv_f.unlink(missing_ok=True)

    # 5. Gerar JSONs para o frontend
    gerar_json_por_municipio(conn)

    conn.close()
    log.info("✓ Atualização concluída!")


if __name__ == "__main__":
    main()
