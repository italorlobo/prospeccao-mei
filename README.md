# 🔍 Sistema de Prospecção MEI — 100% Gratuito e Automático

Cruzamento automático de dados públicos:
- **Receita Federal** — base completa de CNPJ
- **PGFN** — dívida ativa (MEI que deve imposto)
- **Simples Nacional** — flag MEI (`OPCAO_PELO_MEI = S`)
- Filtros por **cidade** (código IBGE) e **estado**
- Botão direto para **WhatsApp** (`https://wa.me/55NUMERO`)
- **Exportação CSV** com todos os dados
- Atualização **automática todo dia 1º do mês** via GitHub Actions

---

## 🚀 Como colocar no ar (passo a passo)

### Passo 1 — Criar conta no GitHub (gratuito)
1. Acesse [github.com](https://github.com) e clique em **Sign up**
2. Crie uma conta com seu e-mail

### Passo 2 — Criar o repositório
1. Clique no botão verde **New** (ou acesse github.com/new)
2. Nome do repositório: `prospeccao-mei`
3. Marque **Public** (obrigatório para o GitHub Pages gratuito)
4. Clique em **Create repository**

### Passo 3 — Fazer upload dos arquivos
1. No repositório criado, clique em **uploading an existing file**
2. Faça upload de **todos** os arquivos desta pasta mantendo a estrutura:
   ```
   prospeccao-mei/
   ├── .github/
   │   └── workflows/
   │       └── atualizar.yml
   ├── scripts/
   │   └── atualizar_dados.py
   ├── docs/
   │   └── index.html
   └── README.md
   ```
3. Clique em **Commit changes**

### Passo 4 — Ativar o GitHub Pages (site gratuito)
1. Vá em **Settings** → **Pages**
2. Em **Source**, selecione: `Deploy from a branch`
3. Em **Branch**, selecione: `main` e pasta `/docs`
4. Clique em **Save**
5. Aguarde ~1 minuto. Seu sistema estará em:
   ```
   https://SEU-USUARIO.github.io/prospeccao-mei/
   ```

### Passo 5 — Executar a primeira atualização
1. Vá em **Actions** no menu do repositório
2. Clique em **Atualização automática das bases MEI**
3. Clique em **Run workflow** → **Run workflow**
4. Aguarde ~30-60 minutos (as bases são grandes)
5. Quando finalizar, os dados reais estarão no sistema!

### A partir daí — tudo automático!
Todo dia 1º do mês às 03:00 (Brasília), o GitHub vai:
- Baixar as bases mais recentes da Receita Federal
- Baixar a lista de dívida ativa da PGFN
- Cruzar os dados
- Atualizar o site automaticamente

---

## 📋 Fontes dos dados

| Base | URL | Atualização |
|------|-----|-------------|
| CNPJ Receita Federal | https://dadosabertos.rfb.gov.br/CNPJ/ | Mensal |
| Simples/MEI | https://dadosabertos.rfb.gov.br/CNPJ/Simples.zip | Mensal |
| Dívida Ativa PGFN | https://portaldatransparencia.gov.br/download-de-dados/pgfn | Mensal |

---

## 🗺️ Códigos IBGE dos municípios

Para adicionar novas cidades, edite o dicionário `MUNICIPIOS` no arquivo
`scripts/atualizar_dados.py`:

```python
MUNICIPIOS = {
    "2051": "João Pessoa",     # PB
    "2110": "Campina Grande",  # PB
    # Adicione aqui:
    "2641": "Recife",          # PE
    "2927408": "Salvador",     # BA
}
```

Encontre o código da sua cidade em:
https://cidades.ibge.gov.br/

---

## 💻 Rodar localmente (opcional)

```bash
# Instalar Python 3.11+
pip install requests

# Clonar o repositório
git clone https://github.com/SEU-USUARIO/prospeccao-mei
cd prospeccao-mei

# Executar atualização
python scripts/atualizar_dados.py

# Abrir o sistema no navegador
open docs/index.html
```

---

## ❓ Dúvidas frequentes

**O sistema é realmente gratuito?**
Sim. GitHub, GitHub Actions e GitHub Pages são 100% gratuitos para
repositórios públicos dentro dos limites de uso (2.000 minutos/mês de
Actions — o script usa cerca de 60 minutos por execução).

**Os dados são em tempo real?**
Não. As bases são atualizadas mensalmente pela Receita Federal e PGFN.
O script baixa sempre a versão mais recente disponível.

**Posso usar para fins comerciais?**
Os dados são públicos e de livre uso. O código deste sistema é MIT.

**Como adicionar mais estados?**
Adicione mais URLs em `RF_CNPJ_URLS` e `RF_ESTAB_URLS` no script Python
(Empresas0 a Empresas9 cobrem o Brasil inteiro).
