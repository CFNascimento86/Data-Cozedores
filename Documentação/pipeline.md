## Pipelines de Dados — Cozedores

Documentação técnica dos pipelines de dados do projeto de Engenharia de Dados Industrial aplicado aos **Cozedores** de uma usina sucroenergética.

Este documento descreve os fluxos de aquisição, tratamento, orquestração, integração Cloud, transformação e consumo analítico dos dados industriais.

---

### 📊 Diagrama Geral dos Pipelines

![Arquitetura](../imagens/arquitetura.jpg)

---

### 🎯 Objetivo

O objetivo dos pipelines é garantir que os dados de processo dos cozedores sejam:

- Coletados de forma contínua
- Persistidos com rastreabilidade
- Tratados e normalizados
- Disponibilizados para operação local
- Exportados para a nuvem
- Transformados em indicadores estratégicos
- Consumidos em dashboards gerenciais

---

### 🧠 Visão Geral dos Fluxos

O projeto possui dois grandes grupos de pipelines:

|          Grupo         |  Ambiente   |    Finalidade    |
|------------------------|-------------|------------------|
| Pipelines Operacionais | On-Premises | Apoio à produção |
| Pipelines Estratégicos | Cloud       | Apoio à gestão   |

---

### 🔄 Fluxo Macro

```text
    Transmissores (Campo)
        ↓
    ET 200M (IM-153)
        ↓
    CLP Siemens S7-315 PN/DP
        ↓
    Edge Node Docker - (snap7_reader.py)
        ↓
    SQL Server RAW
        ↓
-- SQL Server CURATED
|       ↓
|  SQL Server GOLD (Operacional)
|       ↓
|  Dashboards Operacionais
|   
-> SQL Server CURATED
        ↓
    MinIO Bucket
        ↓
    ADLS Bronze
        ↓
    ADLS Silver
        ↓
    ADLS Gold
        ↓
    Power BI
```

---

### 1. Pipeline de Aquisição — snap7_reader.py
O pipeline de aquisição é responsável por coletar os dados diretamente do CLP Siemens S7-315 PN/DP, por meio do protocolo S7.

#### Responsabilidades
- Conectar ao CLP Siemens
- Ler os blocos de dados dos cozedores
- Interpretar os bytes brutos
- Converter os dados para tipos estruturados
- Enviar os registros para o SQL Server
- Registrar logs de execução e falhas

**Entrada**
|         Origem	         |               Descrição                     |
|--------------------------|---------------------------------------------|
| CLP Siemens S7-315 PN/DP | Blocos de dados com variáveis dos cozedores |

**Saída**
|    Destino	  |                 Descrição                      |
|---------------|------------------------------------------------|
|SQL Server RAW |	Dados brutos estruturados para rastreabilidade |

**Exemplo de fluxo interno**
````
Conectar ao CLP
    ↓
Ler DBs configurados
    ↓
Converter bytes para tipos reais
    ↓
Montar payload
    ↓
Inserir no SQL Server RAW
````
---

### 2. Camada RAW — SQL Server
A camada RAW armazena os dados provenientes diretamente do snap7_reader.py.

#### Objetivo
Preservar os dados originais coletados do processo industrial, garantindo rastreabilidade e possibilidade de reprocessamento.

#### Características
- Dados com mínima transformação
- Registro de timestamp de coleta
- Identificação do cozedor
- Identificação da tag
- Valor bruto/interpretado
- Armazenamento histórico
- Base para reprocessamento

---

### 3. Pipeline RAW → CURATED
Este pipeline trata os dados brutos da camada RAW e gera uma base confiável para consumo operacional e analítico.

**DAG ->** *etl_raw_to_curated*

#### Responsabilidades
- Ler dados novos da camada RAW
- Validar estrutura dos registros
- Remover duplicidades
- Tratar valores nulos
- Padronizar nomes de tags
- Padronizar unidades de engenharia
- Identificar valores fora da faixa esperada
- Criar indicadores de qualidade do dado
- Gravar os dados na camada CURATED
  
|     Entrada    |       Saída        |
|----------------|--------------------|
| SQL Server RAW | SQL Server CURATED |

**Transformações típicas**
|    Transformação   	 |              Descrição                         |
|----------------------|------------------------------------------------|
| Deduplicação         |	Remove registros repetidos                    |
| Conversão de tipos   |	Garante tipos numéricos e temporais corretos  |
| Validação de faixa   |	Identifica valores fora do limite operacional |
| Normalização de tags |	Padroniza nomenclaturas                       |
| Enriquecimento       |	Adiciona metadados de cozedor e área          |

---

### 4. Camada CURATED — SQL Server
A camada CURATED contém dados tratados, padronizados e validados.

#### Objetivo
- Consumo operacional local
- Cálculo de KPIs
- Exportação para Cloud
- Reprocessamentos controlados
  
#### Características
- Dados confiáveis
- Padronização de nomenclatura
- Tipagem consistente
- Identificação por equipamento
- Particionamento lógico por data

**Estrutura conceitual**
|   Campo	 |            Descrição            |
|------------|---------------------------------|
| data       | Data usada para particionamento |
| hora       | Horário especifico da medição   |
| cozedor_id | Identificação do cozedor        |
| variáveis	 | Colunas referentes ás variáveis |
| valor      | Valores tratados e normalizados |
| unidade	 | Unidades de engenharia          |
| origem	 | ID de origem da leitura (RAW)   |

---

### 5. Pipeline CURATED → GOLD Operacional
Este pipeline consolida os dados tratados em indicadores de apoio à produção.

**DAG: ->** *etl_curated_to_gold*

#### Responsabilidades
- Ler dados da camada CURATED
- Calcular KPIs operacionais
- Agregar dados por período
- Agregar dados por cozedor
- Disponibilizar dados para dashboards locais
- Registrar histórico de execução
  
|      Entrada       |      Saída      |
|--------------------|-----------------|
| SQL Server CURATED | SQL Server GOLD |

**KPIs operacionais sugeridos**
|          KPI	        |                Descrição                |
|-----------------------|-----------------------------------------|
| Eficiência do cozedor | Indicador de desempenho do equipamento  |
| Consumo de vapor	    | Energia consumida no processo           |
| Tempo de ciclo	    | Duração média do processo de cozimento  |
| Pureza média          | Indicador de qualidade do caldo/produto |
| Desvio operacional	| Tempo ou variáveis fora da faixa ideal  |

**Exemplo de agregações**
| Dimensão |          Agregação        |
|----------|---------------------------|
| Cozedor  | KPI por equipamento       |
| Turno    | KPI por turno operacional |
| Dia	   | KPI diário                |

---

### 6. Pipeline CURATED → CSV Diário
Este pipeline é o ponto de integração entre a camada On-Premises e a camada Cloud.

**DAG: ->** *export_curated_to_minio*

#### Objetivo
Extrair dados particionados por dia da camada CURATED do SQL Server, gerar arquivos .CSV e armazená-los no MinIO.

#### Responsabilidades
- Consultar dados CURATED por data
- Gerar arquivo CSV diário
- Validar quantidade de registros exportados
- Padronizar nome do arquivo
- Armazenar o arquivo no bucket MinIO
- Registrar log da exportação
  
|     Entrada        |     Saída    |
|--------------------|--------------|
| SQL Server CURATED | MinIO Bucket |

**Estrutura no MinIO**
````
minio://cozedores-curated/
 └── ano=2026/
      └── mes=06/
           └── cozedores_curated_2026-06-25.csv
````

---

### 7. Pipeline MinIO → ADLS Bronze
Este pipeline carrega os arquivos .CSV armazenados no MinIO para a camada BRONZE do Azure Data Lake Storage.

**DAG: ->** *etl_minio_to_adls*

#### Responsabilidades
- Identificar arquivos disponíveis no MinIO
- Validar integridade dos arquivos
- Evitar reprocessamento duplicado
- Carregar arquivos no ADLS Bronze
- Registrar metadados da carga

|   Entrada    |    Saída    |
|--------------|-------------|
| MinIO Bucket | ADLS Bronze |

**Estrutura no ADLS Bronze**
````
adls://datalake/cozedores/bronze/
 └── ano=2026/
      └── mes=06/
           └── cozedores_curated_2026-06-25.csv
````

---

### 8. Pipeline Bronze → Silver
A camada Silver representa os dados padronizados, limpos e enriquecidos para uso analítico.

**DAG: ->** *etl_bronze_to_silver*

#### Responsabilidades
- Ler arquivos da camada Bronze
- Converter CSV para formato analítico
- Validar schema
- Corrigir tipos de dados
- Aplicar regras de qualidade
- Padronizar nomes de colunas
- Enriquecer com metadados
- Salvar em formato Parquet
  
|   Entrada   |    Saída    |
|-------------|-------------|
| ADLS Bronze | ADLS Silver |

**Estrutura no ADLS Silver**
````
adls://datalake/cozedores/silver/
 └── ano=2026/
      └── mes=06/
           └── cozedores_2026-06-25.parquet
````

---

### 9. Pipeline Silver → Gold
A camada Gold consolida os dados em estruturas prontas para consumo estratégico.

**DAG: ->** *etl_silver_to_gold*

#### Responsabilidades
- Ler dados tratados da camada Silver
- Calcular KPIs estratégicos
- Criar tabelas agregadas
- Particionar dados por data e cozedor
- Otimizar consumo pelo Power BI

|  Entrada   |    Saída  |
|------------|-----------|
|ADLS Silver | ADLS Gold |

**Datasets sugeridos** 
|          Dataset	         |              Descrição             |
|----------------------------|------------------------------------|
| gold_kpis_cozedores_diario |	KPIs diários por cozedor          |
| gold_kpis_cozedores_turno  |	KPIs por turno                    |
| gold_consumo_vapor         |	Indicadores de consumo energético |
| gold_eficiencia_processo   |	Eficiência consolidada            |
| gold_pureza_caldo          |	Indicadores de qualidade          |

**Estrutura no ADLS Gold**
````
adls://datalake/cozedores/gold/
 └── ano=2026/
      └── mes=06/
           └── cozedores_2026-06-25.parquet
````

---

### 10. Consumo pelo Power BI
O Power BI consome os dados da camada Gold para geração dos dashboards estratégicos.

#### Objetivos
- Acompanhar performance dos cozedores
- Comparar eficiência entre equipamentos
- Avaliar consumo de vapor
- Monitorar pureza
- Visualizar tendências históricas
- Apoiar decisões da gestão
  
**Visões sugeridas**
|  Dashboard  |         	Descrição             |
|-------------|-----------------------------------|
| Visão Geral |	KPIs consolidados da operação     |
| Eficiência  |	Comparativo por cozedor e período |
| Vapor	      | Consumo energético consolidado    |
| Qualidade   |	Pureza e estabilidade do processo |
| Histórico   |	Tendências por safra e período    |

---

### 📌 Conclusão
Os pipelines descritos neste documento conectam o chão de fábrica à camada estratégica de dados.

A arquitetura permite que dados industriais dos cozedores sejam coletados, tratados, historizados e transformados em indicadores para operação e gestão.

Com a combinação de snap7_reader, SQL Server, Apache Airflow, MinIO, ADLS e Power BI, o projeto implementa uma arquitetura híbrida robusta, rastreável e escalável para engenharia de dados industrial.
