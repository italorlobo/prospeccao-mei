"""
Sistema de Prospecção MEI — Script de atualização automática
Usa API pública da Receita Federal (receitaws) + PGFN via dados abertos
"""

import re
import csv
import json
import sqlite3
import logging
import sys
import time
import requests
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DOCS_DIR = BASE_DIR / "docs"
DB_PATH  = DATA_DIR / "prospeccao.db"
DATA_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

# ── Municípios alvo ───────────────────────────────────────────────────────────
MUNICIPIOS = {
    "2051": {"nome": "João Pessoa",    "uf": "PB"},
    "2110": {"nome": "Campina Grande", "uf": "PB"},
    "2180": {"nome": "Patos",          "uf": "PB"},
    "2090": {"nome": "Santa Rita",     "uf": "PB"},
}

# API pública de consulta CNPJ (sem autenticação, rate limit generoso)
RECEITAWS_URL  = "https://receitaws.com.br/v1/cnpj/{cnpj}"
CNPJA_URL      = "https://api.cnpja.com/office/{cnpj}"
BRASIL_API_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

# PGFN dados abertos — CKAN/dados.gov.br
PGFN_CKAN_URL = (
    "https://dados.pgfn.fazenda.gov.br/api/3/action/datastore_search"
    "?resource_id=a0d99f95-0969-4e28-a3e9-f89058e6d9df&limit=5000"
)
PGFN_CSV_URLS = [
    "https://dadosabertos.pgfn.gov.br/Dados_abertos/PGFN/F_DEVEDORES_PGFN_FGTS.zip",
    "https://dadosabertos.pgfn.gov.br/Dados_abertos/PGFN/F_DEVEDORES_PGFN.zip",
]

# CNPJs MEI de João Pessoa via BrasilAPI (busca por município)
BRASIL_API_MEI = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ProspeccaoMEI/2.0; +github.com/italorlobo)",
    "Accept": "application/json",
}

SIT_MAP = {"02": "Ativa", "03": "Suspensa", "04": "Inapta", "08": "Baixada"}


# ── Banco ─────────────────────────────────────────────────────────────────────
def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS mei_cadastro (
        cnpj         TEXT PRIMARY KEY,
        razao_social TEXT,
        municipio    TEXT,
        uf           TEXT,
        sit_cadastral TEXT,
        ddd          TEXT,
        telefone     TEXT,
        email        TEXT,
        opcao_mei    TEXT,
        data_abertura TEXT
    );
    CREATE TABLE IF NOT EXISTS divida_ativa (
        cnpj         TEXT PRIMARY KEY,
        nome_devedor TEXT,
        valor_total  REAL,
        situacao     TEXT,
        uf_devedor   TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_mei_mun ON mei_cadastro(municipio);
    CREATE INDEX IF NOT EXISTS idx_mei_uf  ON mei_cadastro(uf);
    """)
    conn.commit()
    log.info("Banco OK.")


def inserir(conn, tabela, dados, ncols):
    if not dados:
        return
    ph = ",".join(["?"] * ncols)
    conn.executemany(f"INSERT OR REPLACE INTO {tabela} VALUES ({ph})", dados)
    conn.commit()


# ── Baixar PGFN ───────────────────────────────────────────────────────────────
def baixar_pgfn(conn):
    """Tenta baixar dívida ativa PGFN de fontes alternativas."""
    log.info("Buscando dívida ativa PGFN...")

    # Tentativa 1: API CKAN do PGFN
    try:
        r = requests.get(PGFN_CKAN_URL, headers=HEADERS, timeout=30)
        if r.ok:
            data = r.json()
            records = data.get("result", {}).get("records", [])
            if records:
                lote = []
                for rec in records:
                    cnpj = re.sub(r"\D", "", str(rec.get("CPF_CNPJ", "")))
                    if not cnpj:
                        continue
                    try:
                        valor = float(str(rec.get("VALOR_TOTAL", "0")).replace(".","").replace(",",".") or "0")
                    except:
                        valor = 0.0
                    lote.append((
                        cnpj,
                        rec.get("NOME_DEVEDOR", ""),
                        valor,
                        rec.get("SITUACAO_INSCRICAO", ""),
                        rec.get("UF_DEVEDOR", ""),
                    ))
                inserir(conn, "divida_ativa", lote, 5)
                log.info(f"  ✓ PGFN CKAN: {len(lote)} registros")
                return True
    except Exception as e:
        log.warning(f"  PGFN CKAN falhou: {e}")

    # Tentativa 2: ZIP direto
    for url in PGFN_CSV_URLS:
        try:
            import zipfile, io
            log.info(f"  tentando {url}")
            r = requests.get(url, timeout=120, headers=HEADERS, stream=True)
            if r.ok:
                content = r.content
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    for nome in z.namelist():
                        if nome.lower().endswith((".csv",".txt")):
                            with z.open(nome) as f:
                                texto = f.read().decode("latin-1", errors="replace")
                                linhas = texto.splitlines()
                                sep = ";" if linhas[0].count(";") > linhas[0].count(",") else ","
                                reader = csv.DictReader(linhas, delimiter=sep)
                                lote = []
                                for row in reader:
                                    cnpj = re.sub(r"\D", "", row.get("CPF_CNPJ", row.get("CNPJ","")))
                                    if not cnpj: continue
                                    try: valor = float((row.get("VALOR_TOTAL","0") or "0").replace(".","").replace(",","."))
                                    except: valor = 0.0
                                    lote.append((cnpj, row.get("NOME_DEVEDOR",""), valor,
                                                  row.get("SITUACAO_INSCRICAO",""), row.get("UF_DEVEDOR","")))
                                    if len(lote) >= 5000:
                                        inserir(conn, "divida_ativa", lote, 5)
                                        lote = []
                                inserir(conn, "divida_ativa", lote, 5)
                log.info("  ✓ PGFN ZIP baixado")
                return True
        except Exception as e:
            log.warning(f"  ZIP PGFN falhou: {e}")

    log.warning("  PGFN: todas as fontes falharam — sem cruzamento de dívida")
    return False


# ── Buscar MEIs via BrasilAPI ─────────────────────────────────────────────────
def buscar_mei_brasilapi(conn, cnpjs_amostra):
    """
    Consulta BrasilAPI para enriquecer dados de CNPJs MEI conhecidos.
    Rate limit: ~3 req/s
    """
    log.info(f"Enriquecendo {len(cnpjs_amostra)} CNPJs via BrasilAPI...")
    ok = 0
    for cnpj in cnpjs_amostra[:200]:  # limite por execução
        cnpj_limpo = re.sub(r"\D", "", str(cnpj))
        if len(cnpj_limpo) != 14:
            continue
        try:
            r = requests.get(
                BRASIL_API_MEI.format(cnpj=cnpj_limpo),
                headers=HEADERS, timeout=10
            )
            if r.ok:
                d = r.json()
                tel = (d.get("ddd_telefone_1") or "").replace(" ","").replace("-","")
                conn.execute("""
                    INSERT OR REPLACE INTO mei_cadastro VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    cnpj_limpo,
                    d.get("razao_social",""),
                    d.get("municipio",""),
                    d.get("uf",""),
                    d.get("descricao_situacao_cadastral",""),
                    tel[:2] if len(tel) > 8 else "",
                    tel[2:] if len(tel) > 8 else tel,
                    d.get("email","") or "",
                    "S" if d.get("opcao_pelo_mei") else "N",
                    d.get("data_inicio_atividade",""),
                ))
                ok += 1
                if ok % 10 == 0:
                    conn.commit()
                    log.info(f"  {ok} enriquecidos...")
            time.sleep(0.4)  # respeita rate limit
        except Exception as e:
            log.warning(f"  {cnpj_limpo}: {e}")
            time.sleep(1)
    conn.commit()
    log.info(f"  ✓ {ok} CNPJs enriquecidos via BrasilAPI")


# ── Gerar lista de CNPJs MEI de PB via dados.gov.br ──────────────────────────
def buscar_cnpjs_mei_pb(conn):
    """
    Busca lista de CNPJs MEI da Paraíba usando dados.gov.br (CKAN API)
    """
    log.info("Buscando CNPJs MEI da Paraíba via dados.gov.br...")

    # Dataset CNPJ Receita Federal no CKAN
    CKAN_SEARCH = (
        "https://dados.gov.br/api/3/action/datastore_search_sql"
        "?sql=SELECT%20*%20FROM%20%22cnpj%22%20WHERE%20%22UF%22%3D%27PB%27"
        "%20AND%20%22OPCAO_PELO_MEI%22%3D%27S%27%20LIMIT%201000"
    )

    cnpjs_encontrados = []
    try:
        r = requests.get(CKAN_SEARCH, headers=HEADERS, timeout=30)
        if r.ok:
            data = r.json()
            records = data.get("result", {}).get("records", [])
            for rec in records:
                cnpj = re.sub(r"\D", "", str(rec.get("CNPJ_BASICO","") + rec.get("CNPJ_ORDEM","0001") + rec.get("CNPJ_DV","00")))
                if cnpj and len(cnpj) >= 8:
                    cnpjs_encontrados.append(cnpj)
            log.info(f"  CKAN: {len(cnpjs_encontrados)} CNPJs MEI-PB")
    except Exception as e:
        log.warning(f"  CKAN falhou: {e}")

    # Fallback: usa CNPJs conhecidos de devedores PB como semente
    if not cnpjs_encontrados:
        log.info("  fallback: buscando devedores PB na dívida ativa...")
        rows = conn.execute("""
            SELECT cnpj FROM divida_ativa WHERE uf_devedor = 'PB' LIMIT 500
        """).fetchall()
        cnpjs_encontrados = [r[0] for r in rows]
        log.info(f"  {len(cnpjs_encontrados)} CNPJs de devedores PB")

    return cnpjs_encontrados


# ── Gerar JSONs ───────────────────────────────────────────────────────────────
def gerar_jsons(conn):
    indice = []

    for cod, info in MUNICIPIOS.items():
        nome_mun, uf_mun = info["nome"], info["uf"]
        log.info(f"\nGerando JSON: {nome_mun} ({cod})")

        # Busca no cadastro enriquecido
        rows_cad = conn.execute("""
            SELECT cnpj, razao_social, municipio, uf, sit_cadastral,
                   ddd, telefone, email, opcao_mei
            FROM mei_cadastro
            WHERE (municipio LIKE ? OR municipio LIKE ?)
              AND opcao_mei = 'S'
            LIMIT 5000
        """, (f"%{nome_mun}%", f"%JOAO PESSOA%")).fetchall()

        # Cruza com dívida ativa
        registros = []
        for r in rows_cad:
            cnpj, razao, mun, uf, sit, ddd, tel, email, mei = r
            cnpj_limpo = re.sub(r"\D", "", cnpj)

            # Busca dívida
            div = conn.execute(
                "SELECT valor_total, situacao FROM divida_ativa WHERE cnpj = ?",
                (cnpj_limpo,)
            ).fetchone()

            valor    = div[0] if div else 0.0
            sit_div  = div[1] if div else ""
            tel_num  = f"{ddd}{tel}".strip().replace(" ","") if tel else ""

            cnpj_fmt = (f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/"
                        f"{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                        if len(cnpj_limpo) == 14 else cnpj)

            registros.append({
                "cnpj":      cnpj_fmt,
                "nome":      razao,
                "mei":       True,
                "ativo":     "ativa" in (sit or "").lower(),
                "status":    sit or "Ativa",
                "uf":        uf or uf_mun,
                "divida":    valor > 0,
                "valor":     round(valor, 2),
                "sit_divida": sit_div,
                "das":       True,
                "irr":       valor > 0,
                "tel":       tel_num,
                "email":     email or "",
                "wa_link":   f"https://wa.me/55{tel_num}" if tel_num else "",
            })

        # Se não encontrou no cadastro, usa fallback dívida ativa por UF
        if not registros:
            log.warning(f"  sem dados no cadastro — fallback dívida ativa {uf_mun}")
            rows_div = conn.execute("""
                SELECT cnpj, nome_devedor, valor_total, situacao
                FROM divida_ativa
                WHERE uf_devedor = ?
                LIMIT 3000
            """, (uf_mun,)).fetchall()

            for r in rows_div:
                cnpj, nome, valor, sit_div = r
                cnpj_limpo = re.sub(r"\D", "", cnpj)
                cnpj_fmt = (f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/"
                            f"{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                            if len(cnpj_limpo) == 14 else cnpj)
                registros.append({
                    "cnpj": cnpj_fmt, "nome": nome, "mei": True,
                    "ativo": True, "status": "Ativa", "uf": uf_mun,
                    "divida": valor > 0, "valor": round(valor or 0, 2),
                    "sit_divida": sit_div or "", "das": True, "irr": valor > 0,
                    "tel": "", "email": "", "wa_link": "",
                })

        saida = {
            "municipio": nome_mun, "codigo_ibge": cod, "uf": uf_mun,
            "gerado_em": datetime.now().isoformat(),
            "total": len(registros), "registros": registros,
        }
        json_path = DOCS_DIR / f"dados_{cod}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(saida, f, ensure_ascii=False, separators=(",", ":"))
        log.info(f"  → {json_path.name} ({len(registros):,} registros)")
        indice.append({"codigo": cod, "nome": nome_mun, "uf": uf_mun,
                       "arquivo": f"dados_{cod}.json"})

    with open(DOCS_DIR / "municipios.json", "w", encoding="utf-8") as f:
        json.dump(indice, f, ensure_ascii=False, indent=2)
    log.info("\nmunicipos.json atualizado.")


# ── Pipeline ──────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info(f"Prospecção MEI — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 50)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # 1. Dívida ativa PGFN
    baixar_pgfn(conn)

    # 2. Busca CNPJs MEI da PB
    cnpjs = buscar_cnpjs_mei_pb(conn)

    # 3. Enriquece via BrasilAPI (telefone, email, situação)
    if cnpjs:
        buscar_mei_brasilapi(conn, cnpjs)

    # 4. Gera JSONs
    gerar_jsons(conn)

    conn.close()
    log.info("\n✓ Concluído!")


if __name__ == "__main__":
    main()
