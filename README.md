# iFood – Case Técnico
---

Pipeline completo em PySpark + Delta Lake (Bronze → Silver → Silver Quality)
---

<img width="398" height="362" alt="image" src="https://github.com/user-attachments/assets/fcdd9c87-7277-4571-be0d-76efdd3cb494" />

Resultado final da análise de pontos de viagem em relação ao total gasto obtida ao final




## Visão Geral

Esta solução implementa uma arquitetura de dados robusta para ingestão, padronização, validação e disponibilização das corridas de táxis de Nova York (Yellow e Green) entre janeiro e dezembro de 2023.
O foco é garantir **contratos claros**, **idempotência**, **qualidade dos dados** e **camadas bem definidas** para consumo analítico.

A solução utiliza:

* PySpark
* Delta Lake
* Databricks
* Camadas Bronze / Silver / Silver Quality

---

# Arquitetura

## 1. Bronze – Ingestão e Contrato Individual

Cada fonte (Yellow/Green) possui contratos distintos.
O Bronze mantém **fidelidade total ao sistema transacional**.

Características:

* Download dos arquivos originais direto da fonte oficial
* Cast explícito e aplicação de contrato por tipo de táxi
* Deduplicação por chave composta
* Delta Lake particionado por `(year, month)`
* Merge incremental para garantir idempotência
* Preserva diferenças estruturais entre Green e Yellow

Arquivos:

* `BronzeLayer.dbc`
* `Run.dbc` (orquestração)

---

## 2. Silver – Padronização e Modelo Unificado

O Silver consolida as duas origens em um layout único, preservando as diferenças de domínio usando colunas opcionais (`airport_fee`, `trip_type`, etc.).

Principais decisões:

* Normalização de nomes de colunas (`*_pickup_datetime` → `pickup_datetime`)
* Inclusão de campos ausentes com `lit(None)`
* Seleção explícita de colunas seguindo um **schema unificado**
* Unificação via `unionByName`
* Escrita Delta com:

  * `validateSchema=true`
  * `overwriteSchema=false`
* Particionamento por `(year, month, cab_type)`

Arquivo:

* `SilverLayer.dbc`

---

## 3. Silver Quality – Validação Avançada

A camada de qualidade inspeciona a tabela Silver e gera um perfil detalhado do dataset.

Validações implementadas:

* Schema completo esperado
* Nulidade em colunas essenciais
* Regras de domínio (valores negativos inválidos)
* Consistência temporal (dropoff < pickup, corridas > 24h)
* Outliers estruturais por z-score
* Duplicidade de chave composta
* Contagem total e perfilamento básico

Arquivo:

* `SilverLayerQuality.dbc`

---

# Execução

## 1. Orquestração completa

Manualmente, é possível executar cada Notebook seguindo a ordem:
1. src/Setup.dbc para criação das zonas Medallion usadas no Case
2. src/Run.dbc (já importa o notebook BronzeLayer.dbc para popular landing zone e bronze)
3. src/SilverLayer.dbc
4. src/SilverLayerQuality
5. analysis/Perguntas para resultados do Case

Em Job Databricks:
<img width="1133" height="301" alt="image" src="https://github.com/user-attachments/assets/c638734e-1fe2-41ea-91a8-0fb1167e4740" />


---

# Requisitos

* Databricks Community Edition (ou Databricks runtime com Delta)
* Python 3.x
* PySpark
* Delta Lake
* Requests (para download dos parquet)
* Geopandas para análise geográfica de valores e corridas (instalação automática pela pipeline)
* Folium para visualização em mapa de calor na cidade de Nova Iorque (instalação automática pela pipeline)

# Análises Solicitadas pelo Case

## 1. Média de total_amount por mês (Yellow Taxi)

```sql
SELECT
    year,
    month,
    AVG(total_amount) AS media_total_amount
FROM silver_df
WHERE cab_type = 'yellow'
GROUP BY year, month
ORDER BY year, month
```

## 2. Média de passageiros por hora do dia (apenas maio/2023)

```sql
SELECT
    HOUR(pickup_datetime) AS hora,
    AVG(passenger_count) AS media_passageiros
FROM silver_df
WHERE month = 5
GROUP BY HOUR(pickup_datetime)
ORDER BY hora
```

---

# Decisões Técnicas Fundamentais

### 1. Manter contratos separados no Bronze

Green e Yellow têm schemas distintos; unificar no Bronze geraria perda de linhagem e risco de drift.

### 2. Uso de Delta + Merge

Garante idempotência e evita duplicações mesmo se arquivos forem reprocessados.

### 3. Padronização apenas no Silver

A camada Silver é o ponto de convergência analítica, mantendo o Bronze fiel às origens.

### 4. Camada de Qualidade independente

Validações desacopladas permitem auditoria, monitoração e governança.

### 5. Particionamento por metadados críticos

(year, month, cab_type) → garante pushdown eficiente nas análises pedidas.

---

# Possíveis Extensões

* Registro contínuo de erros em `/quality/errors`
* Dashboard de Data Quality
* Testes unitários para contratos e funções de transformação

---


