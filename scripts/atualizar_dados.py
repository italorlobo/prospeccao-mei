"""
Sistema de Prospecção MEI — Script de atualização automática
"""

import re
import csv
import json
import zipfile
import sqlite3
import logging
import sys
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

SIMPLES_URLS = [
    "https://dadosabertos.rfb.gov.br/CNPJ/Simples.zip",
]
EMPRESAS_URLS = [
    "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0_csv.zip",
]
ESTAB_URLS = [
    "https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos0_csv.zip",
]
PGFN_URLS = [
    "https://dadosabertos.pgfn.gov.br/Dados_abertos/PGFN/F_DEVEDORES_PGFN.zip",
    "https://www.portaldatransparencia.gov.br/download-de-dados/pgfn/todos",
]

MUNICIPIOS = {
    "2051": {"nome": "João Pessoa",    "uf": "PB"},
    "2110": {"nome": "Campina Grande", "uf": "PB"},
    "2180": {"nome": "Patos",          "uf": "PB"},
    "2090": {"nome": "Santa Rita",     "uf": "PB"},
}

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"}
SIT_MAP = {"02": "Ativa", "03": "Suspensa", "04": "Inapta", "08": "Baixada"}


def baixar(url, destino, timeout=300):
    destino = Path(destino)
    if destino.exists() and destino.stat().st_size > 50_000:
        log.info(f"  cache: {destino.name} ({destino.stat().st_size//1024//1024} MB)")
        return True
    log.info(f"  GET {url}")
    try:
        with requests.get(url, stream=True, timeout=timeout, headers=HEADERS) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            baixado = 0
            with open(destino, "wb") as f:
                for chunk in r.iter_content(chunk_size=4*1024*1024):
                    f.write(chunk)
                    baixado += len(chunk)
                    if total:
                        print(f"\r  {baixado/total*100:5.1f}% — {baixado//1024//1024}/{total//1024//1024} MB", end="", flush=True)
        print()
        sz = destino.stat().st_size
        if sz < 50_000:
            destino.unlink(missing_ok=True)
            return False
        log.info(f"  ok: {destino.name} ({sz//1024//1024} MB)")
        return True
    except Exception as e:
        log.warning(f"  falhou: {e}")
        destino.unlink(missing_ok=True)
        return False


def baixar_fallback(urls, destino):
    for url in urls:
        if baixar(url, destino):
            return True
    log.error(f"  todas as URLs falharam: {Path(destino).name}")
    return False


def extrair(zip_path, pasta):
    arquivos = []
    try:
        with zipfile.ZipFile(zip_path) as z:
            for nome in z.namelist():
                if nome.lower().endswith((".csv", ".txt")):
                    z.extract(nome, pasta)
                    p = Path(pasta) / nome
                    if p.exists():
                        arquivos.append(p)
                        log.info(f"  extraído: {nome} ({p.stat().st_size//1024//1024} MB)")
    except Exception as e:
        log.error(f"  erro extraindo {zip_path}: {e}")
    return arquivos


def sep(linha):
    return ";" if linha.count(";") >= linha.count(",") else ","


def inserir(conn, tabela, dados, ncols):
    if not dados:
        return
    ph = ",".join(["?"] * ncols)
    try:
        conn.executemany(f"INSERT OR REPLACE INTO {tabela} VALUES ({ph})", dados)
        conn.commit()
    except Exception as e:
        log.warning(f"lote {tabela}: {e}")
        for d in dados:
            try:
                conn.execute(f"INSERT OR REPLACE INTO {tabela} VALUES ({ph})", d)
            except Exception:
                pass
        conn.commit()


def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS simples (
        cnpj_basico TEXT PRIMARY KEY, opcao_simples TEXT, opcao_mei TEXT,
        dt_opcao_mei TEXT, dt_exclusao TEXT
    );
    CREATE TABLE IF NOT EXISTS empresas (
        cnpj_basico TEXT PRIMARY KEY, razao_social TEXT, porte TEXT, opcao_mei TEXT
    );
    CREATE TABLE IF NOT EXISTS estabelecimentos (
        cnpj_basico TEXT, cnpj_ordem TEXT, cnpj_dv TEXT,
        sit_cadastral TEXT, municipio TEXT, uf TEXT,
        ddd1 TEXT, telefone1 TEXT, email TEXT,
        PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
    );
    CREATE TABLE IF NOT EXISTS divida_ativa (
        cnpj TEXT PRIMARY KEY, nome_devedor TEXT, valor_total REAL,
        situacao TEXT, uf_devedor TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_estab_mun ON estabelecimentos(municipio);
    CREATE INDEX IF NOT EXISTS idx_estab_uf  ON estabelecimentos(uf);
    """)
    conn.commit()
    log.info("Banco OK.")


def importar_simples(conn, path):
    log.info(f"Simples: {path.name}")
    n = 0
    with open(path, encoding="latin-1", errors="replace") as f:
        s = sep(f.readline()); f.seek(0)
        lote = []
        for row in csv.reader(f, delimiter=s):
            if len(row) < 4:
                continue
            lote.append((row[0].strip(), row[1].strip(), row[3].strip(),
                          row[4].strip() if len(row) > 4 else "",
                          row[5].strip() if len(row) > 5 else ""))
            if len(lote) >= 10000:
                inserir(conn, "simples", lote, 5); n += len(lote); lote = []
                if n % 500000 == 0: log.info(f"  {n:,}...")
        inserir(conn, "simples", lote, 5); n += len(lote)
    # Garante registro na tabela empresas para cada MEI
    conn.execute("""
        INSERT OR IGNORE INTO empresas (cnpj_basico, razao_social, porte, opcao_mei)
        SELECT cnpj_basico, 'MEI ' || cnpj_basico, 'MEI', opcao_mei
        FROM simples WHERE opcao_mei = 'S'
    """)
    conn.commit()
    mei = conn.execute("SELECT COUNT(*) FROM simples WHERE opcao_mei='S'").fetchone()[0]
    log.info(f"  ✓ {n:,} registros | {mei:,} MEIs")


def importar_empresas(conn, path):
    log.info(f"Empresas: {path.name}")
    n = 0
    with open(path, encoding="latin-1", errors="replace") as f:
        s = sep(f.readline()); f.seek(0)
        lote = []
        for row in csv.reader(f, delimiter=s):
            if len(row) < 7:
                continue
            lote.append((row[0].strip(), row[1].strip(), row[6].strip(), ""))
            if len(lote) >= 10000:
                inserir(conn, "empresas", lote, 4); n += len(lote); lote = []
        inserir(conn, "empresas", lote, 4); n += len(lote)
    conn.execute("""
        UPDATE empresas SET opcao_mei = (
            SELECT opcao_mei FROM simples WHERE simples.cnpj_basico = empresas.cnpj_basico
        ) WHERE EXISTS (SELECT 1 FROM simples WHERE simples.cnpj_basico = empresas.cnpj_basico)
    """)
    conn.commit()
    log.info(f"  ✓ {n:,} empresas")


def importar_estabelecimentos(conn, path):
    log.info(f"Estabelecimentos: {path.name}")
    n = 0
    with open(path, encoding="latin-1", errors="replace") as f:
        s = sep(f.readline()); f.seek(0)
        lote = []
        for row in csv.reader(f, delimiter=s):
            if len(row) < 21:
                continue
            lote.append((
                row[0].strip(), row[1].strip(), row[2].strip(),
                row[5].strip(), row[20].strip(), row[19].strip(),
                row[12].strip(), row[13].strip(),
                row[27].strip() if len(row) > 27 else "",
            ))
            if len(lote) >= 10000:
                inserir(conn, "estabelecimentos", lote, 9); n += len(lote); lote = []
                if n % 500000 == 0: log.info(f"  {n:,}...")
        inserir(conn, "estabelecimentos", lote, 9); n += len(lote)
    log.info(f"  ✓ {n:,} estabelecimentos")


def importar_divida(conn, path):
    log.info(f"Dívida Ativa: {path.name}")
    n = 0
    with open(path, encoding="latin-1", errors="replace") as f:
        s = sep(f.readline()); f.seek(0)
        try:
            reader = csv.DictReader(f, delimiter=s)
            lote = []
            for row in reader:
                cnpj = re.sub(r"\D", "", row.get("CPF_CNPJ", row.get("CNPJ_CPF", row.get("CNPJ", ""))))
                if not cnpj:
                    continue
                try:
                    valor = float((row.get("VALOR_TOTAL", row.get("VALOR_CONSOLIDADO", "0")) or "0").replace(".","").replace(",","."))
                except ValueError:
                    valor = 0.0
                lote.append((cnpj, row.get("NOME_DEVEDOR", row.get("NOME","")).strip(), valor,
                              row.get("SITUACAO_INSCRICAO", row.get("SITUACAO","")).strip(),
                              row.get("UF_DEVEDOR", row.get("UF","")).strip()))
                if len(lote) >= 10000:
                    inserir(conn, "divida_ativa", lote, 5); n += len(lote); lote = []
            inserir(conn, "divida_ativa", lote, 5); n += len(lote)
        except Exception as e:
            log.warning(f"  DictReader falhou: {e} — leitura posicional")
            f.seek(0); lote = []
            for row in csv.reader(f, delimiter=s):
                if len(row) < 2: continue
                cnpj = re.sub(r"\D", "", row[0])
                if not cnpj: continue
                try: valor = float((row[3] if len(row)>3 else "0").replace(".","").replace(",",".") or "0")
                except: valor = 0.0
                lote.append((cnpj, row[1].strip() if len(row)>1 else "", valor,
                              row[4].strip() if len(row)>4 else "",
                              row[5].strip() if len(row)>5 else ""))
                if len(lote) >= 10000:
                    inserir(conn, "divida_ativa", lote, 5); n += len(lote); lote = []
            inserir(conn, "divida_ativa", lote, 5); n += len(lote)
    log.info(f"  ✓ {n:,} devedores")


def gerar_jsons(conn):
    tem_estab = conn.execute("SELECT COUNT(*) FROM estabelecimentos").fetchone()[0] > 0
    tem_nome  = conn.execute("SELECT COUNT(*) FROM empresas WHERE razao_social NOT LIKE 'MEI %'").fetchone()[0] > 0
    log.info(f"Estabelecimentos: {'sim' if tem_estab else 'não'} | Nomes reais: {'sim' if tem_nome else 'não'}")

    indice = []
    for cod, info in MUNICIPIOS.items():
        nome_mun, uf_mun = info["nome"], info["uf"]
        log.info(f"\nJSON {nome_mun} ({cod})")
        registros = []

        if tem_estab:
            rows = []
            for c in [cod, cod.zfill(7), cod.lstrip("0") or "0"]:
                rows = conn.execute("""
                    SELECT est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv,
                           emp.razao_social, est.sit_cadastral, est.uf,
                           est.ddd1, est.telefone1, est.email,
                           d.valor_total, d.situacao
                    FROM estabelecimentos AS est
                    JOIN empresas AS emp ON emp.cnpj_basico = est.cnpj_basico
                    LEFT JOIN divida_ativa AS d
                        ON d.cnpj = (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv)
                    WHERE est.municipio = ? AND emp.opcao_mei = 'S'
                    LIMIT 10000
                """, (c,)).fetchall()
                if rows:
                    log.info(f"  {len(rows):,} MEIs (código '{c}')")
                    break

            for r in rows:
                cnpj_b, cnpj_o, cnpj_dv, razao, sit_cad, uf, ddd, tel, email, valor, sit_div = r
                cnpj_fmt = f"{cnpj_b[:2]}.{cnpj_b[2:5]}.{cnpj_b[5:]}/{cnpj_o}-{cnpj_dv}"
                tel_num = f"{ddd}{tel}".strip().replace(" ","") if tel else ""
                registros.append({
                    "cnpj": cnpj_fmt, "nome": razao or f"MEI {cnpj_b}",
                    "mei": True, "ativo": sit_cad == "02",
                    "status": SIT_MAP.get(sit_cad, "Desconhecida"),
                    "uf": uf or uf_mun,
                    "divida": valor is not None and valor > 0,
                    "valor": round(valor or 0, 2),
                    "sit_divida": sit_div or "",
                    "das": True, "irr": valor is not None and valor > 0,
                    "tel": tel_num, "email": email or "",
                    "wa_link": f"https://wa.me/55{tel_num}" if tel_num else "",
                })
        else:
            # Fallback: Simples + dívida por UF
            rows = conn.execute("""
                SELECT s.cnpj_basico, emp.razao_social, d.valor_total, d.situacao, d.uf_devedor
                FROM simples AS s
                JOIN empresas AS emp ON emp.cnpj_basico = s.cnpj_basico
                LEFT JOIN divida_ativa AS d ON d.cnpj LIKE (s.cnpj_basico || '%')
                WHERE s.opcao_mei = 'S' AND (d.uf_devedor = ? OR d.cnpj IS NULL)
                LIMIT 5000
            """, (uf_mun,)).fetchall()
            log.info(f"  {len(rows):,} MEIs fallback ({uf_mun})")
            for r in rows:
                cnpj_b, razao, valor, sit_div, uf_dev = r
                registros.append({
                    "cnpj": f"{cnpj_b[:2]}.{cnpj_b[2:5]}.{cnpj_b[5:]}/0001-00",
                    "nome": razao or f"MEI {cnpj_b}",
                    "mei": True, "ativo": True, "status": "Ativa",
                    "uf": uf_dev or uf_mun,
                    "divida": valor is not None and valor > 0,
                    "valor": round(valor or 0, 2),
                    "sit_divida": sit_div or "",
                    "das": True, "irr": valor is not None and valor > 0,
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
        indice.append({"codigo": cod, "nome": nome_mun, "uf": uf_mun, "arquivo": f"dados_{cod}.json"})

    with open(DOCS_DIR / "municipios.json", "w", encoding="utf-8") as f:
        json.dump(indice, f, ensure_ascii=False, indent=2)


def main():
    log.info("=" * 50)
    log.info(f"Prospecção MEI — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 50)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    falhou_critico = False

    # 1. Simples/MEI (crítico)
    simples_zip = DATA_DIR / "Simples.zip"
    if baixar_fallback(SIMPLES_URLS, simples_zip):
        for c in extrair(simples_zip, DATA_DIR): importar_simples(conn, c); c.unlink(missing_ok=True)
    else:
        falhou_critico = True

    # 2. Empresas (opcional)
    emp_zip = DATA_DIR / "Empresas0_csv.zip"
    if baixar_fallback(EMPRESAS_URLS, emp_zip):
        for c in extrair(emp_zip, DATA_DIR): importar_empresas(conn, c); c.unlink(missing_ok=True)
    else:
        log.warning("Empresas: usando nomes do Simples")

    # 3. Estabelecimentos (opcional)
    estab_zip = DATA_DIR / "Estabelecimentos0_csv.zip"
    if baixar_fallback(ESTAB_URLS, estab_zip):
        for c in extrair(estab_zip, DATA_DIR): importar_estabelecimentos(conn, c); c.unlink(missing_ok=True)
    else:
        log.warning("Estabelecimentos: modo fallback por UF")

    # 4. Dívida Ativa (opcional)
    pgfn_zip = DATA_DIR / "pgfn.zip"
    if baixar_fallback(PGFN_URLS, pgfn_zip):
        for c in extrair(pgfn_zip, DATA_DIR): importar_divida(conn, c); c.unlink(missing_ok=True)
    else:
        log.warning("PGFN: sem cruzamento de dívida")

    # 5. Gera JSONs sempre
    gerar_jsons(conn)
    conn.close()

    if falhou_critico:
        log.error("Download do Simples.zip falhou — dados não atualizados")
        sys.exit(1)

    log.info("\n✓ Concluído!")


if __name__ == "__main__":
    main()
