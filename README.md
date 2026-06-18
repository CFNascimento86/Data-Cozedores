Engenharia de Dados Industrial - Cozedores

Pipeline híbrido (On-Premises + Cloud) para aquisição, processamento e análise de dados de processo de cozedores em uma usina sucroenergética.

📊 Arquitetura do Projeto

🧠 Visão Geral
Este projeto implementa uma arquitetura de engenharia de dados industrial com dois objetivos principais:

Operacional (On-Premises): Suporte à tomada de decisão em tempo quase real no chão de fábrica

Estratégico (Cloud): Consolidação histórica e analítica para apoio à gestão

Os dados são coletados diretamente dos instrumentos de campo dos cozedores, processados localmente e posteriormente integrados a uma arquitetura moderna de dados em nuvem.

🏭 Camada On-Premises (Operacional)
Responsável pela aquisição, tratamento inicial e disponibilização dos dados para operação.

Fluxo:

Transmissores de campo (pressão, temperatura, nível)

Comunicação via Profibus-PA / Profibus DP

CLP Siemens S7-315-2 PN/DP

Leitura via Edge Node (Docker + snap7)

Persistência no SQL Server (Camada RAW)

Processamento via Apache Airflow

RAW → CURATED

CURATED → GOLD

Cálculo de KPIs (eficiência, pureza, consumo de vapor)


Objetivo:

Garantir suporte operacional à produção com dados confiáveis e estruturados.


☁️ Camada Cloud (Estratégica)

Responsável pela consolidação, historização e análise estratégica dos dados.

Fluxo:

Extração da camada CURATED (SQL Server)

Geração de arquivos CSV particionados por dia

Armazenamento intermediário em MinIO

Ingestão no Azure Data Lake Storage (ADLS)

Arquitetura Medallion:

Bronze: Dados brutos ingeridos

Silver: Dados tratados e normalizados

Gold: Dados agregados e prontos para análise

Consumo:

Power BI

Indicadores estratégicos de performance industrial


🔄 Pipeline de Dados

RAW → CURATED → CSV → MinIO → BRONZE → SILVER → GOLD


⚙️ Tecnologias Utilizadas

Siemens S7-315 (Automação Industrial)

Profibus / Profinet

Python (snap7)

Docker

SQL Server

Apache Airflow

MinIO

Azure Data Lake Storage (ADLS)

Power BI


📁 Estrutura do Projeto

├── README.md
├── assets/
│   ├── arquitetura_hibrida.png
│   ├── arquitetura_onprem.png
│   ├── arquitetura_cloud.png
│
├── docs/
│   ├── arquitetura-geral.md
│   ├── on-premises.md
│   ├── cloud.md
│   ├── pipelines.md
│
├── src/
│   ├── snap7_reader/
│   ├── dags/


🚀 Roadmap

Monitoramento de pipelines (observabilidade)

Data Quality (validação de sensores)

Integração com streaming (Kafka / MQTT)

Modelos preditivos (ML para eficiência industrial)


📌 Considerações

Este projeto representa uma arquitetura híbrida moderna aplicada ao contexto industrial, integrando tecnologias de automação, edge computing e engenharia de dados em nuvem para geração de valor operacional e estratégico.
