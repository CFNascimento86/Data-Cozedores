### Modelo de Dados — Cozedores

Documentação do modelo de dados do projeto de Engenharia de Dados Industrial aplicado aos **Cozedores** de uma usina sucroenergética.

Este documento descreve a estrutura dos dados ao longo de toda a arquitetura, desde a coleta no chão de fábrica até a disponibilização para consumo analítico.

---

#### 🎯 Objetivo

Definir e padronizar a estrutura dos dados em todas as camadas da arquitetura:

- RAW (dados brutos)
- CURATED (dados tratados e normalizados)
- GOLD (dados agregados)
- Medallion (Bronze, Silver, Gold - Cloud)

*Garantindo:*

- Consistência
- Rastreabilidade
- Reprocessamento
- Performance analítica
- Escalabilidade

Cada camada aumenta o nível de qualidade, organização e valor analítico.

---

#### 1. Camada RAW — SQL Server
A camada RAW armazena os dados coletados diretamente do CLP via script snap7_reader.py.

#### *📌 Características*
- Dados com mínima transformação
- Alta granularidade (nível de leitura)
- Estrutura orientada a eventos
- Base para auditoria e reprocessamento

**🗄️ Tabela: raw_cozedores**
````
CREATE TABLE raw_cozedores (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    timestamp_utc DATETIME2 NOT NULL,
    db_number INT NULL,
    fluxo INT NOT NULL,
    cozedor INT NOT NULL,
    temperatura FLOAT NULL,
    pressao_vacuo FLOAT NULL,
    brix FLOAT NULL,
    nivel FLOAT NULL,
    vazao_vapor FLOAT NULL,
    vazao_alimentacao FLOAT NULL,
    estado_batelada INT NULL,
    pureza FLOAT NULL,
    condensado FLOAT NULL,
   );
````

**Estrutura** 
|       Campo       |	  Tipo	 |            Descrição               |
|-------------------|----------|------------------------------------|
| id	              | BIGINT	 | Identificador único incremental    |
| timestamp_utc     | DATETIME | Data/hora da leitura               |
| db-number	        | INT	     | Identificador do Data Block (CLP)  |
| fluxo	            | INT	     | Fluxo que o Cozedor está contido   |
| cozedor	          | INT	     | Número do Cozedor em Campo         |
| temperatura	      | FLOAT    | Variável térmica do processo       |
| pressao_vacuo	    | FLOAT	   | Pressão/Vácuo submetido ao Cozedor |
| brix              |	FLOAT	   | Concentração de sólidos solúveis   |
| nivel             |	FLOAT    | Altura/Quantidade de caldo         |
| vazao_vapor       | FLOAT    | Vazão de vapor no processo         |
| vazao_alimentacao | FLOAT    | Vazão de entrada de caldo          |
| estado_batelada   | INT      | Estágio do processo                |
| pureza            | FLOAT    | % Sacarose / % Brix x 100          |
| condensado        | FLOAT    | Volume excedente da troca térmica  |

---

#### 2. Camada CURATED — SQL Server
A camada CURATED contém dados tratados, padronizados e validados.

#### *📌 Características*
- Tipagem consistente
- Dados limpos
- Padronização de variáveis
- Flags de qualidade
- Base para consumo operacional e exportação
  
**🗄️ Tabela: curated_cozedores**
````
CREATE TABLE curated_cozedores (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    data DATE NOT NULL,
    hora TIME NOT NULL,
    turno CHAR(1) NOT NULL,
    fluxo INT NOT NULL,
    cozedor INT NOT NULL,
    temperatura FLOAT NULL,
    pressao_vacuo FLOAT NULL,
    brix FLOAT NULL,
    nivel FLOAT NULL,
    vazao_vapor FLOAT NULL,
    vazao_alimentacao FLOAT NULL,
    estado_batelada INT NULL,
    pureza FLOAT NULL,
    condensado FLOAT NULL,
	id_raw BIGINT NOT NULL
);
````

**Estrutura** 
|       Campo       |	  Tipo	 |            Descrição               |
|-------------------|----------|------------------------------------|
| id	              | BIGINT	 | Identificador único incremental    |
| data                | DATE     | Data da leitura               |
| hora                | TIME     | Hora da leitura
| turno               | CHAR     | Turno operacional (A-Manhã / B-Tarde / C-Noite) |
| fluxo	            | INT	     | Fluxo que o Cozedor está contido   |
| cozedor	          | INT	     | Número do Cozedor em Campo         |
| temperatura	      | FLOAT    | Variável térmica do processo       |
| pressao_vacuo	    | FLOAT	   | Pressão/Vácuo submetido ao Cozedor |
| brix              |	FLOAT	   | Concentração de sólidos solúveis   |
| nivel             |	FLOAT    | Altura/Quantidade de caldo         |
| vazao_vapor       | FLOAT    | Vazão de vapor no processo         |
| vazao_alimentacao | FLOAT    | Vazão de entrada de caldo          |
| estado_batelada   | INT      | Estágio do processo                |
| pureza            | FLOAT    | % Sacarose / % Brix x 100          |
| condensado        | FLOAT    | Volume excedente da troca térmica  |
| id_raw            | BIGINT   | Identificador único da camada anterior |

---

#### 3. Camada GOLD — SQL Server (Operacional)
Camada voltada para consumo rápido pela operação.

#### *📌 Características*
- Dados agregados
- KPIs calculados
- Baixa latência de consulta
- Foco operacional

**🗄️ Tabela: gold_kpi_cozedores_turno**
````
CREATE TABLE dbo.gold_kpi_cozedores_turno (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    data DATE NOT NULL,
    turno CHAR(1) NOT NULL,
    fluxo INT NOT NULL,
    cozedor INT NOT NULL,
    qtd_registros INT NOT NULL,
    temperatura_media FLOAT NULL,
    pressao_vacuo_media FLOAT NULL,
    brix_medio FLOAT NULL,
    nivel_medio FLOAT NULL,
    vazao_vapor_media FLOAT NULL,
    vazao_alimentacao_media FLOAT NULL,
    pureza_media FLOAT NULL,
    condensado_medio FLOAT NULL,
	ultimo_raw_id BIGINT NOT NULL,
	data_processamento DATETIME2 NOT NULL DEFAULT SYSDATETIME()
	);
````

**Estrutura** 
|       Campo       |	  Tipo	 |            Descrição               |
|-------------------|----------|------------------------------------|
| id	              | BIGINT	 | Identificador único incremental    |
| data                | DATE     | Data da leitura               |
| turno               | CHAR     | Turno operacional (A-Manhã / B-Tarde / C-Noite) |
| fluxo	            | INT	     | Fluxo que o Cozedor está contido   |
| cozedor	          | INT	     | Número do Cozedor em Campo         |
| qtd_registros       | INT      | Quantidade de registros verificados |
| temperatura_media   | FLOAT    | Média da variável térmica do processo |
| pressao_vacuo_media | FLOAT	   | Média de Pressão/Vácuo submetido ao Cozedor |
| brix_medio          |	FLOAT	   | Média de concentração de sólidos solúveis   |
| nivel_medio         |	FLOAT    | Média de Altura/Quantidade de caldo         |
| vazao_vapor_media   | FLOAT    | Média de vazão de vapor no processo         |
| vazao_alimentacao_media | FLOAT    | Média de vazão de entrada de caldo          |
| pureza_media        | FLOAT    | Média de % Sacarose / % Brix x 100          |
| condensado_medio    | FLOAT    | Média de volume excedente da troca térmica  |
| ultimo_id_raw       | BIGINT   | Ultimo identificador único processaso da camada raw |
| data_processamento  | DATETIME2 | Data referente ao último processamento da camada gold |

---

#### 4. Camada Bronze — ADLS
Primeira camada da arquitetura Cloud.

#### *📌 Características*
- Dados em formato CSV
- Estrutura próxima à origem
- Particionamento por data

#### *📁 Estrutura*
````
bronze/
└── ano=YYYY/
    └── mes=MM/
        └── cozedores_curated_YYYY-MM-DD.csv
````
#### *📄 Schema lógico (CSV)*
Segue identico a mesma estrutura da camada CURATED do SQL Server

----

#### 5. Camada Silver — ADLS
Camada de dados tratados e padronizados.

#### *📌 Características*
- Dados limpos
- Tipagem consistente
- Formato Parquet
- Otimizado para leitura analítica
  
#### *📁 Estrutura*
````
silver/
└── cozedor=**/
	└── ano=YYYY/
	    └── mes=MM/
	        └── dia=DD/
            	└── part-0001.parquet
````

#### *📄 Schema lógico (Parquet)*
Segue identico a mesma estrutura da camada Bronze, exceto pela subtração da coluna "data", já que o mesmo está particionado segundo essa premissa.

---

#### 6. Camada Gold — ADLS (Estratégica)
Camada final para consumo analítico.

#### *📌 Características*
- Dados agregados
- Modelagem orientada a negócio
- Alta performance para BI

#### *📁 Estrutura*
````
gold/
└── dataset=cozedores_kpis/
    └── ano=YYYY/
        └── mes=MM/
            └── dia=DD/
                └── part-0001.parquet
````
#### *📄 Schema lógico (Sugerido)*
| Campo	              | Tipo   |    Descrição     |
|---------------------|--------|------------------|
| data_referencia	  | DATE   | Data do KPI      |
| cozedor_id          |	INT	   | Identificador    |
| eficiencia_media    | DOUBLE | Eficiência média |
| consumo_vapor_total |	DOUBLE | Consumo total    |
| pureza_media	      | DOUBLE |  Pureza média    |
| tempo_ciclo_medio   |	DOUBLE | Tempo médio      |
| qtd_registros       |	INT	   | Volume de dados  |

---

#### 📌 Conclusão
O modelo de dados do projeto garante que os dados industriais dos cozedores evoluam de sinais brutos para ativos analíticos estruturados.
A separação em camadas (RAW, CURATED, GOLD e Medallion) permite:
- Escalabilidade
- Reprocessamento
- Governança
- Performance
- Clareza arquitetural
  
Transformando dados operacionais em inteligência estratégica.
