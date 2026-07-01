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

Garantindo:

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
