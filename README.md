# Engenharia de Dados Industrial - Cozedores
Pipeline híbrido (On-Premises + Cloud) para aquisição, processamento e análise de dados de processo de cozedores em uma usina sucroenergética.

---

## 📊 Arquitetura do Projeto
![Arquiteura geral](https://github.com/CFNascimento86/Data-Cozedores/blob/main/imagens/arquitetura_hibrida.jpg)

## 📊 Visão Geral

Este projeto implementa uma arquitetura de engenharia de dados industrial com dois objetivos principais:

- Operacional (On-Premises): Suporte à tomada de decisão em tempo quase real no chão de fábrica

- Estratégico (Cloud): Consolidação histórica e analítica para apoio à gestão

Os dados são coletados diretamente dos instrumentos de campo dos cozedores, processados localmente e posteriormente integrados a uma arquitetura moderna de dados em nuvem.

---

## 🏭 Camada On-Premises (Operacional)
Responsável pela aquisição, tratamento inicial e disponibilização dos dados para operação.

**Fluxo:**

- Transmissores de campo (pressão, temperatura, nível, ...)

- Comunicação via Profibus-PA / Profibus DP / Profinet

- CLP Siemens S7-315-2 PN/DP

- Leitura via Edge Node (Docker + snap7)

- Persistência no SQL Server (Camada RAW)

- Processamento via Apache Airflow

- RAW → CURATED  → GOLD

- Cálculo de KPIs (eficiência, pureza, consumo de vapor, ...)


*Objetivo:*

Garantir suporte operacional à produção com dados confiáveis e estruturados.

---

## ☁️ Camada Cloud (Estratégica)

Responsável pela consolidação, historização e análise estratégica dos dados.

**Fluxo:**
- Extração da camada CURATED (SQL Server)

- Geração de arquivos CSV particionados por dia

- Armazenamento intermediário no MinIO

- Ingestão no Azure Data Lake Storage (ADLS)

- Arquitetura Medallion:

- Bronze: Dados brutos ingeridos

- Silver: Dados tratados e normalizados

- Gold: Dados agregados e prontos para análise

*Consumo:*

Indicadores estratégicos de performance industrial

---

## 🔄 Pipeline de Dados

RAW → CURATED → GOLD → CSV → MinIO → BRONZE → SILVER → GOLD

---

## ⚙️ Tecnologias Utilizadas

- CLP Siemens S7-315 PN/DP (Automação Industrial)

- Profibus / Profinet

- Python (snap7)

- Docker

- SQL Server

- Apache Airflow

- MinIO

- Azure Data Lake Storage (ADLS)


---

## 📁 Estrutura do Projeto
```
├── README.md
|
├── Documentação/
│   ├── on-premises.md
│   ├── cloud.md
│   ├── pipelines.md
│
├── Scripts/
│   ├── snap7_reader/
│   ├── dags/
|
├── imagens/
│   ├── arquitetura_hibrida.jpg
│   ├── arquitetura_onprem.jpg
│   ├── arquitetura_cloud.jpg
```
---

## 📌 Considerações

Este projeto representa uma arquitetura híbrida moderna aplicada ao contexto industrial, integrando tecnologias de automação, edge computing e engenharia de dados em nuvem para geração de valor operacional e estratégico.
