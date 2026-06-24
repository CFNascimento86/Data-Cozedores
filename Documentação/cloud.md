# Arquitetura Cloud — Camada Estratégica

Documentação técnica da camada Cloud do projeto de Engenharia de Dados Industrial aplicado aos **Cozedores** de uma usina sucroenergética.

Esta camada é responsável pela integração dos dados tratados no ambiente On-Premises com uma arquitetura analítica em nuvem, baseada no padrão **Medallion Architecture**: Bronze, Silver e Gold.

---

## 📊 Diagrama da Arquitetura Cloud

![Arquitetura Cloud](imagens/arquitetura_cloud.png)

---

## 🎯 Objetivo

A camada Cloud tem como objetivo consolidar, organizar, historizar e disponibilizar os dados industriais dos cozedores para análises estratégicas de gestão.

Enquanto a camada **On-Premises** atende decisões operacionais da produção, a camada **Cloud** atende decisões corporativas, gerenciais e analíticas.

---

## 🧠 Visão Geral

O fluxo Cloud inicia-se a partir dos dados já tratados na camada **CURATED** do SQL Server On-Premises.

A partir dessa camada, uma DAG no Apache Airflow realiza:

1. Extração dos dados particionados por dia
2. Conversão dos dados para arquivos `.CSV`
3. Armazenamento intermediário em um bucket no **MinIO**
4. Posterior carga dos arquivos para a camada **BRONZE** do **Azure Data Lake Storage**
5. Transformações para as camadas **SILVER** e **GOLD**
6. Consumo analítico via **Power BI**

---

## 🔄 Fluxo Geral da Camada Cloud

```text
SQL Server CURATED
        │
        │ DAG Airflow - Export CSV
        ▼
MinIO Bucket
        │
        │ DAG Airflow - Load to ADLS
        ▼
ADLS BRONZE
        │
        │ Tratamento e Padronização
        ▼
ADLS SILVER
        │
        │ Agregação e Particionamento
        ▼
ADLS GOLD
        │
        │ Consumo Analítico
        ▼
Power BI
```
---

### 🧩 Papel da Camada Cloud na Arquitetura Híbrida
A arquitetura do projeto é híbrida porque combina:

|   Ambiente  |	                 Papel	                           | Decisão Apoiada |
|-------------|----------------------------------------------------|-----------------|
| On-Premises |	Aquisição, tratamento inicial e apoio operacional	 | Produção        |
| Cloud       |	Historização, consolidação e análises estratégicas | Gestão          |

A camada Cloud recebe dados industriais já tratados localmente e os transforma em ativos analíticos para acompanhamento estratégico da performance dos cozedores.
