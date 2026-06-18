graph TD
    %% Cores e Estilos
    classDef otLayer fill:#e8f4f8,stroke:#2b7b98,stroke-width:2px;
    classDef edgeLayer fill:#f9ede6,stroke:#b95c27,stroke-width:2px;
    classDef itLayer fill:#eef9eb,stroke:#3b8b22,stroke-width:2px;
    classDef db fill:#f0eeff,stroke:#5544aa,stroke-width:2px;

    subgraph Chão de Fábrica / OT Layer
        A[Instrumentos de Campo<br>Transmissores de Variáveis]:::otLayer
        B[Remota Siemens<br>ET 200/M IM 153-1]:::otLayer
        C[CPU Siemens<br>S7-315-2 PN/DP]:::otLayer

        A -- Profibus-PA --> B
        B -- Profibus DP --> C
        C -. Lógica Interna .-> C_DB[(Organiza Variáveis <br>DB101 a DB205)]:::db
    end

    subgraph Edge Computing Layer
        D[Node Industrial Edge<br>Gateway / Mini PC]:::edgeLayer
        D1([Docker: snap7_reader])
        D2([Docker: logger])
        D3([Docker: healthcheck])

        C -- Profinet / Industrial Ethernet<br>S7 Protocol PUT/GET --> D
        D --- D1
        D --- D2
        D --- D3
        D1 -. Conversão Byte para .-> D1_Parse[REAL / INT / BOOL]
    end

    subgraph TI & Engenharia de Dados
        E[(SQL Server<br>Camada RAW)]:::db
        F[Apache Airflow<br>Orquestração de Dados]:::itLayer
        F1(DAG: RAW to CURATED)
        F2(DAG: CURATED to GOLD)
        F3(DAG: Cálculos de KPIs<br>Eficiência, Pureza, Vapor)

        D1 -- TCP/IP<br>Payload Estruturado JSON/SQL --> E
        E -- Rede Corporativa / DMZ --> F
        F --- F1
        F --- F2
        F --- F3
    end
