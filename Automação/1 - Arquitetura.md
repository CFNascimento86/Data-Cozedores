````
                  ┌──────────────────────────────────────────┐
                  │        USINA DE AÇÚCAR – COZEDORES       │
                  └──────────────────────────────────────────┘

┌───────────────────────────────┐        ┌───────────────────────────────┐
│        FLUXO 1 (5 Cozedores)  │        │        FLUXO 2 (5 Cozedores)  │
│                               │        │                               │
│ Coz 01…05 → ET 200M(IM153-1)  │        │ Coz 06…10 → ET 200M(IM153-1)  │
└───────────────┬───────────────┘        └───────────────┬───────────────┘
                │                                        │
                │ PROFIBUS DP                            │ PROFIBUS DP
                │                                        │
        ┌───────▼────────────────────────────────────────▼─────── ┐
        │                 CPU Siemens S7‑315‑2 PN/DP              │
        │  - Lê todos os sinais dos 10 cozedores                  │
        │  - Organiza variáveis em DBs estruturados (DB101–DB205) │
        │  - Comunicação S7 Protocol habilitada (PUT/GET)         │
        └───────────────┬─────────────────────────────────────────┘
                        │
                        │ PROFINET / Industrial Ethernet
                        │
        ┌───────────────▼────────────────────────────────────────────┐
        │                     EDGE COMPUTING NODE                    │
        │        (Mini PC Industrial / VM / Gateway Industrial)      │
        ├────────────────────────────────────────────────────────────┤
        │  Docker Containers:                                        │
        │                                                            │
        │  • snap7_reader                                            │
        │      - Lê DBs dos cozedores via S7 Protocol                │
        │      - Converte bytes → REAL/INT/BOOL                      │
        │      - Gera payload estruturado                            │
        │      - Envia para SQL Server (RAW)                         │
        │                                                            │
        │  • logger                                                  │
        │      - Registra falhas, reconexões, tempos de ciclo        │
        │                                                            │
        │  • healthcheck                                             │
        │      - Monitora latência e disponibilidade do CLP          │
        └───────────────┬────────────────────────────────────────────┘
                        │
                        │ TCP/IP
                        │
        ┌───────────────▼────────────────────────────────────────────┐
        │                         SQL SERVER                         │
        │                                                            │
        │  Esquema RAW:                                              │
        │     raw_cozedores                                          │
        │     raw_cozedores_json                                     │
        │                                                            │
        │  (Armazena dados brutos de todos os 10 cozedores)          │
        └───────────────┬────────────────────────────────────────────┘
                        │
                        │ Rede Corporativa / DMZ
                        │
        ┌───────────────▼────────────────────────────────────────────┐
        │                           AIRFLOW                          │
        │                                                            │
        │  DAGs:                                                     │
        │   • ETL_RAW_to_CURATED                                     │
        │   • ETL_CURATED_to_GOLD                                    │
        │   • KPIs (Eficiência, Pureza, Consumo de Vapor, etc.)      │
        │   • Limpeza e Retenção                                     │
        └───────────────┬────────────────────────────────────────────┘
                        │
                        │
        ┌───────────────▼────────────────────────────────────────────┐
        │                     DASHBOARDS / BI / API                  │
        │                                                            │
        │  • Power BI                                                │
        │  • Grafana                                                 │
        │  • API Industrial (REST)                                   │
        │                                                            │
        │  Consumo direto da camada GOLD                             │
        └────────────────────────────────────────────────────────────┘
````
