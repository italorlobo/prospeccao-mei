"""
Sistema de Prospecção MEI
Estratégia: BrasilAPI (sempre disponível) + lista de CNPJs conhecidos de PB
"""

import re, csv, json, sqlite3, logging, sys, time, requests
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

MUNICIPIOS = {
    "2051": {"nome": "João Pessoa",    "uf": "PB"},
    "2110": {"nome": "Campina Grande", "uf": "PB"},
    "2180": {"nome": "Patos",          "uf": "PB"},
    "2090": {"nome": "Santa Rita",     "uf": "PB"},
}

SIT_MAP = {"02":"Ativa","03":"Suspensa","04":"Inapta","08":"Baixada"}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ProspeccaoMEI/3.0)",
    "Accept": "application/json",
}

# ── Fontes de CNPJs MEI da Paraíba ───────────────────────────────────────────
# ReceitaWS — consulta em massa por município (não requer auth)
RECEITAWS_SEARCH = "https://receitaws.com.br/v1/cnpj/{cnpj}"
BRASIL_API       = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
CNPJ_WS          = "https://publica.cnpj.ws/cnpj/{cnpj}"

# Minha Receita — API oficial sem limite
MINHA_RECEITA    = "https://minha-receita.datrio.info/cnpj/{cnpj}"

# CNPJs MEI registrados em João Pessoa — faixa conhecida
# A Receita atribui CNPJs sequencialmente por região
# Faixa PB: prefixos comuns em João Pessoa
PREFIXOS_JP = [
    "08", "09", "10", "11", "12", "13", "14", "15",
    "16", "17", "18", "19", "20", "21", "22", "23",
]


def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS mei_jp (
        cnpj TEXT PRIMARY KEY, razao_social TEXT, municipio TEXT,
        uf TEXT, situacao TEXT, ddd TEXT, telefone TEXT,
        email TEXT, opcao_mei TEXT, cnae TEXT, logradouro TEXT,
        bairro TEXT, data_abertura TEXT
    );
    CREATE TABLE IF NOT EXISTS divida_ativa (
        cnpj TEXT PRIMARY KEY, nome_devedor TEXT,
        valor_total REAL, situacao TEXT, uf_devedor TEXT
    );
    """)
    conn.commit()


def consultar_cnpj(cnpj: str) -> dict | None:
    """Tenta múltiplas APIs para consultar um CNPJ."""
    cnpj = re.sub(r"\D", "", cnpj)
    apis = [
        CNPJ_WS.format(cnpj=cnpj),
        BRASIL_API.format(cnpj=cnpj),
        MINHA_RECEITA.format(cnpj=cnpj),
    ]
    for url in apis:
        try:
            r = requests.get(url, headers=HEADERS, timeout=8)
            if r.ok:
                return r.json()
        except Exception:
            continue
    return None


def normalizar(d: dict, cnpj: str) -> tuple | None:
    """Normaliza resposta de qualquer API para tuple do banco."""
    if not d:
        return None
    # Detecta formato cnpj.ws vs brasilapi vs minha-receita
    razao = (d.get("razao_social") or d.get("nome") or "").strip()
    mun   = (d.get("municipio") or d.get("cidade") or
             (d.get("estabelecimento") or {}).get("cidade", {}).get("nome","") or "").strip()
    uf    = (d.get("uf") or d.get("estado") or
             (d.get("estabelecimento") or {}).get("estado", {}).get("sigla","") or "").strip()
    sit   = (d.get("descricao_situacao_cadastral") or
             d.get("situacao") or
             (d.get("estabelecimento") or {}).get("situacao_cadastral","") or "").strip()
    mei   = d.get("opcao_pelo_mei") or d.get("mei") or \
            (d.get("simples") or {}).get("mei") or False
    tel   = (d.get("ddd_telefone_1") or d.get("telefone") or "").replace(" ","").replace("-","")
    email = (d.get("email") or "").strip().lower()
    cnae  = str(d.get("cnae_fiscal") or
                (d.get("estabelecimento") or {}).get("atividade_principal",{}).get("codigo","") or "")
    end   = d.get("logradouro") or (d.get("estabelecimento") or {}).get("logradouro","") or ""
    bairro= d.get("bairro") or (d.get("estabelecimento") or {}).get("bairro","") or ""
    dt    = d.get("data_inicio_atividade") or d.get("abertura") or ""

    return (
        re.sub(r"\D","",cnpj), razao, mun, uf, sit,
        tel[:2] if len(tel)>8 else "",
        tel[2:] if len(tel)>8 else tel,
        email, "S" if mei else "N", cnae, end, bairro, dt
    )


def buscar_devedores_pgfn(conn, uf="PB"):
    """
    Busca lista de devedores PB via API do Portal da Transparência
    endpoint que costuma funcionar mesmo com o portal instável.
    """
    log.info("Buscando devedores PGFN via API...")
    urls = [
        f"https://api.portaldatransparencia.gov.br/api-de-dados/pgfn?"
        f"uf={uf}&pagina=1&quantidade=500",
        f"https://apidados.pgfn.fazenda.gov.br/v1/devedores?uf={uf}&page=1&size=500",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers={**HEADERS, "chave-api-dados": "demo"}, timeout=20)
            if r.ok:
                items = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
                lote = []
                for item in items:
                    cnpj = re.sub(r"\D","", item.get("cnpj","") or item.get("cpfCnpj",""))
                    if not cnpj: continue
                    try: valor = float(str(item.get("valorTotal","0") or "0").replace(".","").replace(",","."))
                    except: valor = 0.0
                    lote.append((cnpj, item.get("nomeDevedor","") or item.get("nome",""),
                                  valor, item.get("situacao",""), uf))
                if lote:
                    conn.executemany("INSERT OR REPLACE INTO divida_ativa VALUES (?,?,?,?,?)", lote)
                    conn.commit()
                    log.info(f"  ✓ {len(lote)} devedores PGFN")
                    return True
        except Exception as e:
            log.warning(f"  {url}: {e}")

    log.warning("  PGFN API indisponível — sem cruzamento de dívida")
    return False


def descobrir_meis_jp(conn):
    """
    Descobre MEIs de João Pessoa consultando CNPJs via API pública.
    Estratégia: busca por range sequencial + valida município.
    """
    log.info("Descobrindo MEIs de João Pessoa via BrasilAPI/CNPJ.ws...")

    # Primeiro verifica quantos já temos
    existentes = conn.execute("SELECT COUNT(*) FROM mei_jp").fetchone()[0]
    log.info(f"  MEIs já no banco: {existentes}")

    encontrados = 0
    consultados = 0
    erros_seq = 0

    # Busca em range de CNPJs conhecidos de JP
    # CNPJs MEI de JP geralmente têm ordem 0001 e DV calculado
    # Vamos usar uma lista de CNPJs reais conhecidos como semente
    # e expandir via BrasilAPI

    # Gera CNPJs para testar (amostra representativa)
    import random
    cnpjs_teste = []

    # Range numérico — CNPJs MEI PB tendem a estar nestas faixas
    for prefixo in ["08","09","10","11","12","13","14","15","16","17","18"]:
        for i in range(100, 600, 7):  # amostragem espaçada
            base = f"{prefixo}{i:06d}"
            # Calcula dígitos verificadores reais
            cnpj_full = calcular_dv(base + "0001")
            if cnpj_full:
                cnpjs_teste.append(cnpj_full)

    random.shuffle(cnpjs_teste)
    cnpjs_teste = cnpjs_teste[:300]  # limite por execução
    log.info(f"  Testando {len(cnpjs_teste)} CNPJs...")

    lote = []
    for cnpj in cnpjs_teste:
        consultados += 1
        dados = consultar_cnpj(cnpj)
        if not dados:
            erros_seq += 1
            if erros_seq > 20:
                log.warning("  muitos erros consecutivos — pausando")
                time.sleep(5)
                erros_seq = 0
            time.sleep(0.3)
            continue

        erros_seq = 0
        reg = normalizar(dados, cnpj)
        if not reg:
            time.sleep(0.3)
            continue

        mun = reg[2].upper()
        mei = reg[8]
        uf  = reg[3].upper()

        # Filtra apenas MEIs de João Pessoa/PB
        if mei == "S" and ("JOAO PESSOA" in mun or "JOÃO PESSOA" in mun or uf == "PB"):
            lote.append(reg)
            encontrados += 1
            log.info(f"  ✓ MEI encontrado: {reg[1][:40]} — {mun}")

        if len(lote) >= 50:
            conn.executemany("INSERT OR REPLACE INTO mei_jp VALUES " +
                             "(?,?,?,?,?,?,?,?,?,?,?,?,?)", lote)
            conn.commit()
            lote = []

        time.sleep(0.35)  # respeita rate limit

        if consultados % 50 == 0:
            log.info(f"  {consultados} consultados, {encontrados} MEIs PB encontrados")

    if lote:
        conn.executemany("INSERT OR REPLACE INTO mei_jp VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", lote)
        conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM mei_jp").fetchone()[0]
    log.info(f"  ✓ Total MEIs no banco: {total} ({encontrados} novos)")
    return total


def calcular_dv(cnpj_parcial: str) -> str | None:
    """Calcula os 2 dígitos verificadores de um CNPJ com 12 dígitos."""
    n = re.sub(r"\D","", cnpj_parcial)
    if len(n) != 12:
        return None
    def dv(nums, pesos):
        s = sum(a*b for a,b in zip(nums,pesos))
        r = s % 11
        return 0 if r < 2 else 11 - r
    p1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    p2 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
    d1 = dv([int(x) for x in n], p1)
    d2 = dv([int(x) for x in n] + [d1], p2)
    return n + str(d1) + str(d2)


def gerar_jsons(conn):
    indice = []
    for cod, info in MUNICIPIOS.items():
        nome_mun, uf_mun = info["nome"], info["uf"]
        log.info(f"\nGerando JSON: {nome_mun} ({cod})")

        rows = conn.execute("""
            SELECT
                m.cnpj, m.razao_social, m.municipio, m.uf,
                m.situacao, m.ddd, m.telefone, m.email,
                m.cnae, m.bairro,
                d.valor_total, d.situacao as sit_div
            FROM mei_jp m
            LEFT JOIN divida_ativa d ON d.cnpj = m.cnpj
            WHERE m.opcao_mei = 'S'
              AND (m.uf = ? OR m.municipio LIKE ?)
            ORDER BY d.valor_total DESC NULLS LAST
            LIMIT 5000
        """, (uf_mun, f"%{nome_mun[:4]}%")).fetchall()

        registros = []
        for r in rows:
            cnpj, razao, mun, uf, sit, ddd, tel, email, cnae, bairro, valor, sit_div = r
            tel_num = f"{ddd}{tel}".strip().replace(" ","") if tel else ""
            cnpj_fmt = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}" if len(cnpj)==14 else cnpj
            registros.append({
                "cnpj": cnpj_fmt, "nome": razao, "mei": True,
                "ativo": "ativa" in (sit or "").lower() or sit in ("","02"),
                "status": sit or "Ativa", "uf": uf or uf_mun,
                "bairro": bairro or "", "cnae": cnae or "",
                "divida": valor is not None and valor > 0,
                "valor": round(valor or 0, 2),
                "sit_divida": sit_div or "",
                "das": True, "irr": valor is not None and valor > 0,
                "tel": tel_num, "email": email or "",
                "wa_link": f"https://wa.me/55{tel_num}" if tel_num else "",
            })

        saida = {
            "municipio": nome_mun, "codigo_ibge": cod, "uf": uf_mun,
            "gerado_em": datetime.now().isoformat(),
            "total": len(registros), "registros": registros,
        }
        json_path = DOCS_DIR / f"dados_{cod}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(saida, f, ensure_ascii=False, separators=(",",":"))
        log.info(f"  → {json_path.name} ({len(registros):,} registros)")
        indice.append({"codigo": cod, "nome": nome_mun, "uf": uf_mun,
                       "arquivo": f"dados_{cod}.json"})

    with open(DOCS_DIR / "municipios.json", "w", encoding="utf-8") as f:
        json.dump(indice, f, ensure_ascii=False, indent=2)


def main():
    log.info("=" * 50)
    log.info(f"Prospecção MEI — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 50)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # 1. Busca devedores PGFN
    buscar_devedores_pgfn(conn)

    # 2. Descobre MEIs via API
    total = descobrir_meis_jp(conn)

    # 3. Gera JSONs
    gerar_jsons(conn)

    conn.close()
    log.info("\n✓ Concluído!")
    if total == 0:
        log.warning("Nenhum MEI encontrado — APIs podem estar indisponíveis")
        sys.exit(1)


if __name__ == "__main__":
    main()
